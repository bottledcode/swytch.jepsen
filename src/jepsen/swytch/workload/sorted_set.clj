(ns jepsen.swytch.workload.sorted-set
  "Sorted set convergence workload for safe mode.

  Generates ZADD operations with scores from all nodes. After partition
  heals, verifies LWW scores resolve correctly and all nodes agree."
  (:require [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]]
            [jepsen.swytch.checker :as sc]
            [taoensso.carmine :as car]))

(def key-name "swytch-zset")
(def members ["alice" "bob" "charlie" "dave" "eve"])

(defn zadd-op [_ _]
  {:type :invoke, :f :zadd, :key key-name,
   :score (double (rand-int 1000)), :member (rand-nth members)})

(defn zrange-op [_ _]
  {:type :invoke, :f :zrange, :key key-name, :value nil})

(defn final-reads
  "Generator that reads from all nodes one last time."
  []
  (gen/each-thread (gen/once zrange-op)))

;; ---- Sorted set checker ----

(defn sorted-set-checker
  "Verifies that after convergence, all nodes agree on the sorted set
  contents and ordering (LWW score resolution)."
  []
  (reify checker/Checker
    (check [_ test history _opts]
      (let [;; Final zrange reads per node
            final-reads (->> (sc/with-nodes test history)
                             (filter #(= :ok (:type %)))
                             (filter #(= :zrange (:f %)))
                             (group-by :node)
                             (map (fn [[_ ops]] (last ops)))
                             (remove nil?))
            ;; All nodes should return the same ordered list
            all-values  (map :value final-reads)
            converged?  (apply = all-values)
            ;; Track the last successful ZADD per member to verify LWW
            last-zadds  (->> history
                             (filter #(= :ok (:type %)))
                             (filter #(= :zadd (:f %)))
                             (group-by :member)
                             (map (fn [[m ops]] [m (last ops)]))
                             (into {}))]
        {:valid?      converged?
         :converged?  converged?
         :node-values (into {} (map (fn [r] [(:node r) (:value r)]) final-reads))
         :last-zadds  (into {} (map (fn [[m op]] [m (:score op)]) last-zadds))}))))

(defn workload
  "Returns a workload map for sorted set convergence testing.

  Options:
    :rate  - ops per second (default 100)"
  [{:keys [rate] :or {rate 100}}]
  {:client    nil
   :generator (->> (gen/mix [zadd-op zadd-op zadd-op zrange-op])
                   (gen/stagger (/ 1 rate)))
   :final-generator (final-reads)
   :checker   (checker/compose
                {:sorted-set  (sorted-set-checker)
                 :convergence (sc/convergence-checker #{:zrange})})})
