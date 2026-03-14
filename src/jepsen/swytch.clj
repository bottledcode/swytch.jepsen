(ns jepsen.swytch
  "Entry point for the Jepsen test suite for Swytch."
  (:require [clojure.tools.logging :refer [info]]
            [jepsen [cli :as cli]
                    [checker :as checker]
                    [client :as client]
                    [control :as c]
                    [generator :as gen]
                    [nemesis :as nemesis]
                    [tests :as tests]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.os.debian :as debian]
            [jepsen.swytch.db :as sdb]
            [taoensso.carmine :as car]))

;; ---- Redis client via Carmine ----

(defn conn-spec
  "Returns a Carmine connection spec for the given node."
  [node]
  {:pool {}
   :spec {:host node
          :port sdb/redis-port}})

(defn redis-get [conn key]
  (car/wcar conn (car/get key)))

(defn redis-set [conn key value]
  (car/wcar conn (car/set key value)))

;; ---- Jepsen Client ----

(defrecord SwytchClient [conn node]
  client/Client
  (open! [this test node]
    (assoc this
           :conn (conn-spec node)
           :node node))

  (setup! [this test])

  (invoke! [this test op]
    (try
      (case (:f op)
        :read  (let [v (redis-get conn (str (:key op)))]
                 (assoc op :type :ok :value (when v (parse-long v))))
        :write (do (redis-set conn (str (:key op)) (str (:value op)))
                   (assoc op :type :ok)))
      (catch Exception e
        (assoc op :type :info :error (.getMessage e)))))

  (teardown! [this test])

  (close! [this test]))

;; ---- Operation generators ----

(defn r [_ _] {:type :invoke, :f :read, :key "x", :value nil})
(defn w [_ _] {:type :invoke, :f :write, :key "x", :value (rand-int 100)})

;; ---- Test definition ----

(defn swytch-test
  "Constructs a Jepsen test map for Swytch."
  [opts]
  (let [ca-material    (atom {})
        noise-keys     (atom {})
        cluster-config (atom nil)]
    (merge tests/noop-test
           opts
           {:name           "swytch"
            :os             debian/os
            :db             (sdb/db opts)
            :ca-material    ca-material
            :noise-keys     noise-keys
            :cluster-config cluster-config
            :client         (map->SwytchClient {})
            :nemesis        nemesis/noop
            :checker        (checker/compose
                              {:timeline (timeline/html)})
            :generator      (->> (gen/mix [r w])
                                 (gen/stagger 1)
                                 (gen/time-limit (:time-limit opts 30)))})))

;; ---- CLI ----

(def cli-opts
  "Additional CLI options for Swytch tests."
  [[nil "--swytch-binary PATH" "Path to the Swytch binary"
    :default "/home/withinboredom/code/cloxcache/swytch"]
   [nil "--noise-keygen-binary PATH" "Path to the noise-keygen binary"
    :default nil]])

(defn -main
  "CLI entry point."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn   swytch-test
                                         :opt-spec  cli-opts})
                   (cli/serve-cmd))
            args))
