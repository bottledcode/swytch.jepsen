(ns jepsen.swytch.cluster-config
  "Generates Swytch cluster.yml from Jepsen's node topology.

  Nodes are split into two regions: the first half goes into region-a,
  the remainder into region-b.  Each node gets a deterministic integer
  ID via `node->id`."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [jepsen [control :as c]]))

(def config-dir  "/etc/swytch")
(def config-path (str config-dir "/cluster.yml"))

(defn node->id
  "Maps a Jepsen node name to a 1-based integer node ID.
  Deterministic: sorted order of the full node list."
  [test node]
  (let [sorted (sort (:nodes test))]
    (inc (.indexOf (vec sorted) node))))

(defn node-region
  "Assigns a node to one of two regions based on its position.
  First ceil(n/2) nodes go to region-a, rest to region-b."
  [test node]
  (let [sorted   (sort (:nodes test))
        idx      (.indexOf (vec sorted) node)
        midpoint (int (Math/ceil (/ (count sorted) 2.0)))]
    (if (< idx midpoint) "region-a" "region-b")))

(defn noise-public-key
  "Derives the Noise protocol public key from a node's TLS cert.
  Requires the noise-keygen binary to be on the node (uploaded with Swytch)."
  [node]
  (let [cert-path (str "/etc/swytch/certs/" node "-cert.pem")
        key-path  (str "/etc/swytch/certs/" node "-key.pem")]
    (str/trim (c/exec "/opt/swytch/noise-keygen"
                      cert-path
                      key-path))))

(defn gen-cluster-config
  "Generates the cluster.yml content string for the given test.
  `noise-keys` is a map of node -> hex noise public key."
  [test noise-keys]
  (let [sorted (sort (:nodes test))
        node-entries
        (str/join
          "\n"
          (for [node sorted]
            (let [id     (node->id test node)
                  region (node-region test node)
                  nkey   (get noise-keys node "")]
              (str "    - id: " id "\n"
                   "      address: " node ":7000\n"
                   "      region: " region "\n"
                   "      noise_public_key: \"" nkey "\""))))]
    (str "cluster:\n"
         "  nodes:\n"
         node-entries "\n"
         "\n"
         "storage:\n"
         "  upload_endpoint: \"http://" (first sorted) ":9080\"\n"
         "  cdn_endpoint: \"http://" (first sorted) ":9080\"\n"
         "  access_key: \"jepsen-test\"\n")))

(defn install-config!
  "Writes the cluster.yml to the node."
  [config-content]
  (c/su
    (c/exec :mkdir :-p config-dir)
    (c/exec :bash :-c
            (str "cat > " config-path " <<'CFGEOF'\n"
                 config-content
                 "\nCFGEOF"))))
