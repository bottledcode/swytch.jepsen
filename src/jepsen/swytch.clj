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
            [jepsen.swytch.client :as swytch-client]
            [jepsen.swytch.db :as sdb]))

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
            :client         (swytch-client/client)
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
