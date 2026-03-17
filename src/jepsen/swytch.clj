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
            [jepsen.swytch.checker :as sc]
            [jepsen.swytch.client :as swytch-client]
            [jepsen.swytch.db :as sdb]
            [jepsen.swytch.nemesis :as sn]
            [jepsen.swytch.os :as swytch-os]
            [jepsen.swytch.workload.counter :as counter]
            [jepsen.swytch.workload.set :as set-wl]
            [jepsen.swytch.workload.sorted-set :as sorted-set]))

(def workloads
  "Map of workload names to constructor functions."
  {:counter    counter/workload
   :set        set-wl/workload
   :sorted-set sorted-set/workload})

(def nemesis-configs
  "Map of nemesis configuration names to constructor functions.
  Phase 1: only :none and :safe are available (single-region, no Cloud)."
  {:none        (fn [_] {:kill-pkg  nil
                         :fault-pkg {:nemesis         nemesis/noop
                                     :generator       nil
                                     :final-generator nil
                                     :perf            #{}}})
   :safe        (fn [db] (sn/safe-nemesis db))})

(defn swytch-test
  "Constructs a Jepsen test map for Swytch."
  [opts]
  (let [ca-material     (atom {})
        noise-keys      (atom {})
        cluster-config  (atom nil)
        built?          (atom false)
        test-opts       (atom nil)
        db              (sdb/db opts)
        workload-name   (keyword (:workload opts "counter"))
        workload-fn     (get workloads workload-name)
        _               (when-not workload-fn
                          (throw (ex-info (str "Unknown workload: " workload-name)
                                         {:available (keys workloads)})))
        workload        (workload-fn opts)
        nemesis-name    (keyword (:nemesis-config opts "safe"))
        nemesis-fn      (get nemesis-configs nemesis-name)
        _               (when-not nemesis-fn
                          (throw (ex-info (str "Unknown nemesis config: " nemesis-name)
                                         {:available (keys nemesis-configs)})))
        nemesis-pkg     (nemesis-fn db)
        ;; safe-nemesis returns {:kill-pkg ... :fault-pkg ...}
        ;; :none returns a simple package — wrap it for compatibility
        kill-pkg        (:kill-pkg nemesis-pkg)
        fault-pkg       (:fault-pkg nemesis-pkg)
        ;; Compose nemeses from both packages into one.
        ;; nemesis/noop doesn't support Reflection (fs), so guard against that.
        fs-or-empty (fn [nem] (try (nemesis/fs nem) (catch Exception _ #{})))
        combined-nemesis (nemesis/compose
                           (merge {}
                             (when kill-pkg
                               (into {} (map (fn [f] [f (:nemesis kill-pkg)])
                                             (fs-or-empty (:nemesis kill-pkg)))))
                             (when fault-pkg
                               (into {} (map (fn [f] [f (:nemesis fault-pkg)])
                                             (fs-or-empty (:nemesis fault-pkg)))))))]
    (merge tests/noop-test
           opts
           {:name           (str "swytch-" (name workload-name))
            :os             swytch-os/os
            :db             db
            :ca-material    ca-material
            :noise-keys     noise-keys
            :cluster-config cluster-config
            :built?         built?
            :test-opts      test-opts
            :client         (swytch-client/client)
            :nemesis        combined-nemesis
            :checker        (checker/compose
                              (merge
                                {:timeline             (timeline/html)
                                 :workload             (:checker workload)
                                 :availability         (sc/availability-checker)}
                                (when (not= nemesis-name :none)
                                  {:partition-effective (sc/partition-effective-checker)})))
            :generator      (sn/phase-generator
                              {:normal-secs (:normal-secs opts 10)
                               :fault-secs  (:fault-secs opts 30)
                               :settle-secs (:settle-secs opts 10)}
                              nemesis-pkg
                              (:generator workload)
                              (:final-generator workload))
            :perf           (into #{} (concat (:perf kill-pkg) (:perf fault-pkg)))})))

;; ---- CLI ----

(def cli-opts
  "Additional CLI options for Swytch tests."
  [[nil "--swytch-source PATH" "Path to Swytch source directory (builds automatically)"
    :default "/home/withinboredom/code/cloxcache"]
   [nil "--swytch-binary PATH" "Path to a pre-built Swytch binary (skips build)"
    :default nil]
   [nil "--noise-keygen-binary PATH" "Path to a pre-built noise-keygen binary"
    :default nil]
   [nil "--workload NAME" "Workload to run: counter, set, sorted-set"
    :default "counter"]
   [nil "--nemesis-config NAME" "Nemesis config: none, safe"
    :default "safe"]
   [nil "--rate NUM" "Ops per second"
    :default 100
    :parse-fn #(Long/parseLong %)]
   [nil "--normal-secs NUM" "Seconds of normal operation before faults"
    :default 10
    :parse-fn #(Long/parseLong %)]
   [nil "--fault-secs NUM" "Seconds of fault injection"
    :default 30
    :parse-fn #(Long/parseLong %)]
   [nil "--settle-secs NUM" "Seconds to settle after healing (needs time for anti-entropy)"
    :default 30
    :parse-fn #(Long/parseLong %)]
   [nil "--debug" "Enable debug logging on Swytch nodes"
    :default false]])

(defn -main
  "CLI entry point."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn   swytch-test
                                         :opt-spec  cli-opts})
                   (cli/serve-cmd))
            args))
