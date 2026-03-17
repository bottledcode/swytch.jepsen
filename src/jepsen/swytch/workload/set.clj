(ns jepsen.swytch.workload.set
  "Set convergence workload for safe mode.

  Generates SADD with unique values from all nodes. After partition heals,
  verifies every :ok add appears in final SMEMBERS from every node."
  (:require [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]]
            [jepsen.swytch.checker :as sc]
            [taoensso.carmine :as car]))

(def key-name "swytch-set")

(let [counter (atom 0)]
  (defn add-op [_ _]
    {:type :invoke, :f :sadd, :key key-name, :value [(swap! counter inc)]})

  (defn read-op [_ _]
    {:type :invoke, :f :smembers, :key key-name, :value nil}))

(defn final-reads
  "Generator that reads from all nodes one last time."
  []
  (gen/each-thread (gen/once read-op)))

;; ---- Set convergence checker ----

(defn set-convergence-checker
  "Checks that every successfully added element appears in the final
  SMEMBERS read from every node, and all nodes return identical sets."
  []
  (reify checker/Checker
    (check [_ test history _opts]
      (let [ok-adds    (->> history
                            (filter #(= :ok (:type %)))
                            (filter #(= :sadd (:f %)))
                            (mapcat :value)
                            set)
            ;; Final reads: last smembers per node
            final-reads (->> (sc/with-nodes test history)
                             (filter #(= :ok (:type %)))
                             (filter #(= :smembers (:f %)))
                             (group-by :node)
                             (map (fn [[_ ops]] (last ops)))
                             (remove nil?))
            ;; Check each node has all added elements
            per-node   (into {}
                             (for [read final-reads]
                               [(:node read)
                                {:value   (:value read)
                                 :missing (clojure.set/difference
                                            ok-adds
                                            (set (map #(if (string? %) (parse-long %) %) (:value read))))
                                 :extra   (clojure.set/difference
                                            (set (map #(if (string? %) (parse-long %) %) (:value read)))
                                            ok-adds)}]))
            any-missing (some #(seq (:missing (val %))) per-node)
            ;; Convergence: all nodes have identical sets
            all-values  (map #(set (:value %)) final-reads)
            converged?  (apply = all-values)]
        {:valid?     (and (not any-missing) converged?)
         :ok-adds    (count ok-adds)
         :per-node   per-node
         :converged? converged?}))))

(defn workload
  "Returns a workload map for set convergence testing.

  Options:
    :rate  - ops per second (default 100)"
  [{:keys [rate] :or {rate 100}}]
  {:client    nil
   :generator (->> (gen/mix [add-op add-op add-op read-op])
                   (gen/stagger (/ 1 rate)))
   :final-generator (final-reads)
   :checker   (checker/compose
                {:set-convergence (set-convergence-checker)
                 :convergence     (sc/convergence-checker #{:smembers})})})
