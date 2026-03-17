(ns jepsen.swytch.db
  "Jepsen DB lifecycle for Swytch: installs the binary, generates certs
  and cluster config, starts/stops the process, and collects logs."
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure.string :as str]
            [jepsen [control :as c]
                    [db :as db]
                    [util :as util]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [jepsen.swytch.cert :as cert]
            [jepsen.swytch.cluster-config :as cc])
  (:import [java.io File]))

(def swytch-dir   "/opt/swytch")
(def binary       (str swytch-dir "/swytch"))
(def keygen-bin   (str swytch-dir "/noise-keygen"))
(def pid-file     (str swytch-dir "/swytch.pid"))
(def log-file     (str swytch-dir "/swytch.log"))
(def effects-path (str swytch-dir "/data/effects.log"))
(def tiplog-path  (str swytch-dir "/data/tiplog"))
(def data-dir     (str swytch-dir "/data"))

(def redis-port 6379)
(def cluster-port 7000)

(defn build-swytch!
  "Builds the Swytch binary and noise-keygen locally from source.
  Returns a map of {:swytch path, :noise-keygen path}."
  [source-dir]
  (let [source-dir (str source-dir)]
    (info "Building Swytch from" source-dir)
    (let [swytch-out (str source-dir "/swytch")
          keygen-out (str source-dir "/noise-keygen")
          env        {"GOOS" "linux" "GOARCH" "amd64" "CGO_ENABLED" "0"}
          run!       (fn [& args]
                       (let [pb (ProcessBuilder. (into-array String args))
                             pe (.environment pb)]
                         (.directory pb (File. source-dir))
                         (doseq [[k v] env] (.put pe k v))
                         (.redirectErrorStream pb true)
                         (let [p  (.start pb)
                               out (slurp (.getInputStream p))
                               rc  (.waitFor p)]
                           (when (not= 0 rc)
                             (throw (ex-info (str "Build failed: " out)
                                            {:exit rc :output out}))))))]
      (run! "go" "build" "--tags" "nolicense" "-o" swytch-out ".")
      (run! "go" "build" "-o" keygen-out "./cmd/noise-keygen/main.go")
      (info "Build complete")
      {:swytch       swytch-out
       :noise-keygen keygen-out})))

(defn local-md5
  "Returns the md5 hex digest of a local file."
  [path]
  (let [md (java.security.MessageDigest/getInstance "MD5")
        buf (byte-array 8192)]
    (with-open [is (java.io.FileInputStream. (str path))]
      (loop []
        (let [n (.read is buf)]
          (when (pos? n)
            (.update md buf 0 n)
            (recur)))))
    (apply str (map #(format "%02x" %) (.digest md)))))

(defn install-binary!
  "Stops any running swytch, uploads the binary and noise-keygen to the
  node, and verifies the remote binary matches the local build via md5."
  [test]
  (c/su
    ;; Stop the running process first — can't overwrite a running binary
    (cu/stop-daemon! binary pid-file)
    (c/exec :rm :-f binary)
    (c/exec :mkdir :-p swytch-dir)
    (c/exec :mkdir :-p data-dir)
    (c/upload (:swytch-binary test) binary)
    (c/exec :chmod "+x" binary)
    ;; Verify the upload matches the local build
    (let [local-hash  (local-md5 (:swytch-binary test))
          remote-hash (first (str/split (c/exec :md5sum binary) #"\s+"))]
      (when (not= local-hash remote-hash)
        (throw (ex-info "Binary mismatch after upload!"
                        {:local  local-hash
                         :remote remote-hash
                         :binary binary}))))
    (when-let [kg (:noise-keygen-binary test)]
      (c/upload kg keygen-bin)
      (c/exec :chmod "+x" keygen-bin))))

(defn start-swytch!
  "Starts the Swytch process on this node."
  [test node]
  (let [node-id (cc/node->id test node)
        base-args ["redis"
                   "--port"                (str redis-port)
                   "--bind"                "0.0.0.0"
                   "--maxmemory"           "5gb"
                   "--node-id"             (str node-id)
                   "--effects-path"        "memory"
                   "--tip-log-path"        tiplog-path
                   "--config"              cc/config-path
                   "--cluster-cert-file"   (str cert/cert-dir "/" node "-cert.pem")
                   "--cluster-key-file"    (str cert/cert-dir "/" node "-key.pem")
                   "--cluster-ca-cert-file" (str cert/cert-dir "/ca.pem")
                   "--log-format"          "json"]
        args (if (:debug test)
               (into ["redis" "--debug"] (rest base-args))
               base-args)]
    (info "Starting Swytch on" node "with node-id" node-id)
    (apply cu/start-daemon!
      {:logfile log-file
       :pidfile pid-file
       :chdir   swytch-dir}
      binary
      args)))

(defn stop-swytch!
  "Stops the Swytch process."
  []
  (cu/stop-daemon! binary pid-file))

(defn wipe-data!
  "Removes effects log, tiplog, and any other data files."
  []
  (c/su
    (c/exec :rm :-rf data-dir)
    (c/exec :mkdir :-p data-dir)))

(defn setup-certs!
  "Sets up TLS certificates for the cluster. The first node generates
  the CA, then distributes it to all other nodes. Each node generates
  its own node cert signed by the CA."
  [test node]
  (let [first-node (first (sort (:nodes test)))]
    (if (= node first-node)
      ;; First node: generate CA
      (do
        (info "Generating CA on" node)
        (cert/gen-ca!)
        ;; Store CA material in test atom for other nodes
        (let [ca-cert (cert/read-file (str cert/cert-dir "/ca.pem"))
              ca-key  (cert/read-file (str cert/cert-dir "/ca-key.pem"))]
          (swap! (:ca-material test) assoc :cert ca-cert :key ca-key)))
      ;; Other nodes: wait for CA, then install it
      (do
        (info "Waiting for CA material on" node)
        (util/await-fn
          (fn [] (when-not (:cert @(:ca-material test))
                   (throw (RuntimeException. "CA not ready yet"))))
          {:retry-interval 1000
           :log-interval   5000
           :log-message    "Waiting for CA cert..."
           :timeout        30000})
        (let [{:keys [cert key]} @(:ca-material test)]
          (cert/install-ca-from! cert key))))
    ;; Every node: generate its own node cert
    (info "Generating node cert for" node)
    (cert/gen-node-cert! node)))

(defn setup-cluster-config!
  "Generates noise keys and writes cluster.yml on this node.
  The first node collects all noise keys, generates the config,
  and stores it in the test atom. Other nodes wait and install."
  [test node]
  (let [first-node (first (sort (:nodes test)))
        ;; Every node derives its own noise key
        nkey (cc/noise-public-key node)]
    ;; Store this node's noise key
    (swap! (:noise-keys test) assoc node nkey)
    (info "Node" node "noise key:" nkey)

    (if (= node first-node)
      ;; First node: wait for all keys, then generate config
      (do
        (util/await-fn
          (fn [] (when (< (count @(:noise-keys test)) (count (:nodes test)))
                   (throw (RuntimeException. "Not all noise keys collected"))))
          {:retry-interval 1000
           :log-interval   5000
           :log-message    "Waiting for noise keys..."
           :timeout        60000})
        (let [config (cc/gen-cluster-config test @(:noise-keys test))]
          (swap! (:cluster-config test) (constantly config))
          (cc/install-config! config)))
      ;; Other nodes: wait for config
      (do
        (util/await-fn
          (fn [] (when-not @(:cluster-config test)
                   (throw (RuntimeException. "Cluster config not ready"))))
          {:retry-interval 1000
           :log-interval   5000
           :log-message    "Waiting for cluster config..."
           :timeout        60000})
        (cc/install-config! @(:cluster-config test))))))

(defrecord SwytchDB [swytch-binary noise-keygen-binary]
  db/DB
  (setup! [this test node]
    ;; Build once (first node to reach this point builds; others wait)
    (locking build-swytch!
      (when (and (:swytch-source test)
                 (not (:swytch-binary test))
                 (not @(:built? test)))
        (let [bins (build-swytch! (:swytch-source test))]
          (swap! (:test-opts test) assoc
                 :swytch-binary       (:swytch bins)
                 :noise-keygen-binary (:noise-keygen bins))
          (reset! (:built? test) true))))
    ;; Use built paths if available
    (let [test (if-let [opts @(:test-opts test)]
                 (merge test opts)
                 test)]
      (wipe-data!)
      (c/su (c/exec :rm :-f log-file))
      (install-binary! test)
      (setup-certs! test node)
      (setup-cluster-config! test node)
      (start-swytch! test node)
      (Thread/sleep 5000)))

  (teardown! [this test node]
    (stop-swytch!)
    (wipe-data!)
    (c/su
      (c/exec :rm :-rf cert/cert-dir)
      (c/exec :rm :-rf cc/config-dir)))

  db/Kill
  (kill! [this test node]
    (info "Killing Swytch on" node)
    (cu/stop-daemon! binary pid-file))

  (start! [this test node]
    (info "Restarting Swytch on" node)
    (start-swytch! test node))

  db/Pause
  (pause! [this test node]
    (c/su (cu/grepkill! :stop binary)))

  (resume! [this test node]
    (c/su (cu/grepkill! :cont binary)))

  db/LogFiles
  (log-files [this test node]
    [log-file]))

(defn db
  "Constructs a SwytchDB instance. Options:

    :swytch-binary         - path to local swytch binary to upload
    :noise-keygen-binary   - path to local noise-keygen binary to upload"
  [opts]
  (map->SwytchDB (select-keys opts [:swytch-binary :noise-keygen-binary])))
