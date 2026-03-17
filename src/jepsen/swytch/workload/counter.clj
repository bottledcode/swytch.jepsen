(ns jepsen.swytch.workload.counter
  "Counter convergence workload for safe mode.

  Generates INCR operations on a counter key from all nodes concurrently.
  After partition heals + settle, verifies:
    - Final counter value on every node = sum of all :ok increments
    - All nodes agree on the same value (convergence)"
  (:require [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]]
            [jepsen.swytch.checker :as sc]
            [taoensso.carmine :as car]))

(def key-name "swytch-counter")

(defn incr-op  [_ _] {:type :invoke, :f :incr,  :key key-name, :value nil})
(defn read-op  [_ _] {:type :invoke, :f :read,  :key key-name, :value nil})

(defn final-reads
  "Generator that reads from all nodes one last time."
  []
  (gen/each-thread (gen/once read-op)))

(defn workload
  "Returns a workload map for counter convergence testing.

  Options:
    :rate  - ops per second (default 100)"
  [{:keys [rate] :or {rate 100}}]
  {:client    nil ;; Uses the shared SwytchClient
   :generator (->> (gen/mix [incr-op incr-op incr-op read-op])
                   (gen/stagger (/ 1 rate)))
   :final-generator (final-reads)
   :checker   (checker/compose
                {:crdt-counter (sc/crdt-counter-checker)
                 :convergence  (sc/convergence-checker)})})
