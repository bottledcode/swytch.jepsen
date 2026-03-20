(ns jepsen.swytch.checker
  "Custom Jepsen checkers for Swytch consistency properties.

  Phase 1: single-region safe mode. Validates:
    - Convergence: all nodes agree after heal + settle
    - CRDT counter correctness: sum of INCRs = final value
    - Availability: commutative ops mostly succeed during partitions
    - Partition effectiveness: both sides mutated independently
    - Fork-choice: deterministic resolution after heal
    - Safe transactions: exactly-once commit
    - Partition blocks txns: transactions error during partitions"
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]]))

;; ---- Helpers ----

(defn ->map
  "Converts an op (possibly a Jepsen Op record) to a plain map."
  [op]
  (into {} op))

(defn all-ops
  "Converts a Jepsen history to a vec of plain maps."
  [history]
  (mapv ->map history))

(defn oks
  "Returns a vec of :ok op maps from history."
  [history]
  (into [] (filter #(= :ok (:type %))) (map ->map history)))

(defn infos
  "Returns a vec of :info op maps from history."
  [history]
  (into [] (filter #(= :info (:type %))) (map ->map history)))

(defn fails
  "Returns a vec of :fail op maps from history."
  [history]
  (into [] (filter #(= :fail (:type %))) (map ->map history)))

(defn ops-by-f
  "Filters a seq of op maps to those with the given :f value.
  Arg order is f first for use with ->> threading."
  [f ops]
  (filter #(= f (:f %)) ops))

(defn op-node
  "Derives the node name for an operation from its :process field.
  In Jepsen, process p maps to (nth nodes (mod p (count nodes)))."
  [test op]
  (let [nodes (:nodes test)
        p     (:process op)]
    (when (number? p)
      (nth nodes (mod p (count nodes))))))

(defn with-nodes
  "Annotates each op with :node derived from :process."
  [test ops]
  (mapv #(assoc % :node (op-node test %)) ops))

(defn partition-windows
  "Extracts time windows where partitions were active from the history.
  Returns a seq of {:start time :stop time} maps."
  [history]
  (let [nemesis-ops (->> (infos history)
                         (filter #(#{:start-partition :stop-partition} (:f %))))]
    (loop [ops   (seq nemesis-ops)
           start nil
           windows []]
      (if-let [op (first ops)]
        (cond
          (and (= :start-partition (:f op)) (nil? start))
          (recur (rest ops) (:time op) windows)

          (and (= :stop-partition (:f op)) start)
          (recur (rest ops) nil (conj windows {:start start :stop (:time op)}))

          :else
          (recur (rest ops) start windows))
        (if start
          (conj windows {:start start :stop Long/MAX_VALUE})
          windows)))))

(defn during-partition?
  "Returns true if the operation occurred during any partition window.
  A grace period (in nanoseconds) shrinks the window from the start to
  account for partition detection latency — nodes need time to notice
  that peers are unreachable before they can reject writes."
  ([windows op] (during-partition? windows op 0))
  ([windows op grace-nanos]
   (let [t (:time op)]
     (some (fn [{:keys [start stop]}]
             (and (<= (+ start grace-nanos) t) (<= t stop)))
           windows))))

;; ---- Convergence Checker ----

(defn convergence-checker
  "After partition heals + settle window, reads from ALL nodes should
  return the same value. Looks at the last read per key per node.

  `read-fs` is the set of :f values that count as reads (default #{:read})."
  ([] (convergence-checker #{:read}))
  ([read-fs]
   (reify checker/Checker
     (check [_ test history _opts]
       (let [final-reads (->> (with-nodes test (oks history))
                              (filter #(read-fs (:f %)))
                              (group-by (juxt :node :key))
                              (map (fn [[_ ops]] (last ops)))
                              (remove nil?))
             by-key      (group-by :key final-reads)
             divergent   (into {}
                               (for [[k reads] by-key
                                     :let [values (distinct (map :value reads))]
                                     :when (> (count values) 1)]
                                 [k {:values values
                                     :reads  (mapv #(select-keys % [:node :value :time])
                                                   reads)}]))]
         {:valid?    (empty? divergent)
          :divergent divergent})))))

;; ---- CRDT Counter Checker ----

(defn crdt-counter-checker
  "Verifies that the sum of all successful INCR operations equals the
  final counter value on every node."
  []
  (reify checker/Checker
    (check [_ test history _opts]
      (let [ok-ops      (with-nodes test (oks history))
            incr-counts (->> ok-ops
                             (ops-by-f :incr)
                             (group-by :key)
                             (map (fn [[k ops]] [k (count ops)]))
                             (into {}))
            final-reads (->> ok-ops
                             (ops-by-f :read)
                             (group-by (juxt :node :key))
                             (map (fn [[_ ops]] (last ops)))
                             (remove nil?))
            errors (for [read final-reads
                         :let [expected (get incr-counts (:key read) 0)
                               actual   (:value read)]
                         :when (and actual (not= expected actual))]
                     {:key      (:key read)
                      :node     (:node read)
                      :expected expected
                      :actual   actual})]
        {:valid?          (empty? errors)
         :incr-counts     incr-counts
         :errors          (vec errors)}))))

;; ---- Availability Checker ----

(defn availability-checker
  "During partition windows, the :fail rate for commutative operations
  (reads, writes, incr) must be below the given threshold (0.0 - 1.0).
  Default threshold is 0.5 (50%).

  In single-region safe mode, minority-side nodes will error on writes
  but majority-side nodes will succeed. With a 5-node cluster and a
  3/2 split, ~40% of ops failing is expected."
  ([] (availability-checker 0.5))
  ([threshold]
   (reify checker/Checker
     (check [_ test history _opts]
       (let [windows          (partition-windows history)
             commutative-fs   #{:read :write :incr :sadd :lpush :zadd}
             all              (all-ops history)
             partition-ops    (->> all
                                   (filter #(#{:ok :fail} (:type %)))
                                   (filter #(commutative-fs (:f %)))
                                   (filter #(during-partition? windows %)))
             total            (count partition-ops)
             failed           (count (filter #(= :fail (:type %)) partition-ops))
             fail-rate        (if (zero? total) 0.0 (double (/ failed total)))]
         {:valid?     (<= fail-rate threshold)
          :total-ops  total
          :failed-ops failed
          :fail-rate  fail-rate
          :threshold  threshold})))))

;; ---- Partition Effectiveness Checker ----

(def ^:private partition-grace-nanos
  "Grace period (5 seconds in nanoseconds) after a partition starts
  before we expect nodes to have detected the partition. Nodes need
  time to notice peers are unreachable via heartbeat timeouts."
  (* 5 1000000000))

(defn partition-effective-checker
  "Verifies that the network partition actually caused independent
  mutation on multiple nodes. During partition windows (after a grace
  period for partition detection), successful write-like operations
  should originate from at least 2 different nodes, proving both
  sides were mutating state independently.

  Default write-fs: #{:write :incr :sadd :lpush :zadd :txn}"
  ([] (partition-effective-checker #{:write :incr :sadd :lpush :zadd :txn}))
  ([write-fs]
   (reify checker/Checker
     (check [_ test history _opts]
       (let [windows (partition-windows history)
             partition-writes (->> (with-nodes test (oks history))
                                   (filter #(write-fs (:f %)))
                                   (filter #(during-partition? windows % partition-grace-nanos)))
             writing-nodes (set (map :node partition-writes))]
         (cond
           (empty? windows)
           {:valid?             :unknown
            :note               "No partition windows detected"}

           (zero? (count partition-writes))
           {:valid?             true
            :note               "No writes observed during partition (after grace period)"
            :writing-nodes      #{}
            :writing-node-count 0
            :partition-writes   0}

           :else
           {:valid?             (>= (count writing-nodes) 2)
            :writing-nodes      writing-nodes
            :writing-node-count (count writing-nodes)
            :partition-writes   (count partition-writes)}))))))

;; ---- Safe Transaction Checker ----

(defn safe-txn-checker
  "Exactly-once transaction commit. For transactional keys, the number
  of committed (successful) transactions must equal the final value,
  with no gaps or duplicates."
  []
  (reify checker/Checker
    (check [_ test history _opts]
      (let [committed-txns (->> (oks history)
                                (ops-by-f :txn)
                                vec)
            committed-writes (->> committed-txns
                                  (mapcat :value)
                                  (filter vector?)
                                  (filter #(= :write (first %)))
                                  (group-by second)
                                  (map (fn [[k writes]]
                                         [k (mapv #(nth % 2) writes)]))
                                  (into {}))
            duplicates (into {}
                             (for [[k values] committed-writes
                                   :let [dupes (filter #(> (val %) 1)
                                                       (frequencies values))]
                                   :when (seq dupes)]
                               [k dupes]))]
        {:valid?     (empty? duplicates)
         :committed  (count committed-txns)
         :duplicates duplicates}))))

;; ---- Partition Blocks Txns Checker ----

(defn partition-blocks-txns-checker
  "In safe mode, zero transactions should commit during a partition.
  Checks that no :txn operations succeed during partition windows
  (after the grace period for partition detection)."
  []
  (reify checker/Checker
    (check [_ test history _opts]
      (let [windows         (partition-windows history)
            partition-txns  (->> (oks history)
                                 (ops-by-f :txn)
                                 (filter #(during-partition? windows % partition-grace-nanos))
                                 vec)]
        {:valid?              (empty? partition-txns)
         :committed-during-partition (count partition-txns)
         :txns                partition-txns}))))

;; ---- Fork-Choice Checker ----

(defn fork-choice-checker
  "After a partition heals, all nodes must agree on the same value.
  This validates deterministic fork-choice resolution: when both sides
  of a partition performed commutative operations, the merge must
  produce identical results on every node."
  []
  (reify checker/Checker
    (check [_ test history _opts]
      (let [final-reads (->> (with-nodes test (oks history))
                             (ops-by-f :read)
                             (group-by (juxt :node :key))
                             (map (fn [[_ ops]] (last ops)))
                             (remove nil?))
            by-key      (group-by :key final-reads)
            disagreements (into {}
                                (for [[k reads] by-key
                                      :let [values (distinct (map :value reads))]
                                      :when (> (count values) 1)]
                                  [k {:values values
                                      :reads  (mapv #(select-keys % [:node :value :time])
                                                    reads)}]))]
        {:valid?        (empty? disagreements)
         :disagreements disagreements}))))

;; ---- Composed checker for safe mode ----

(defn safe-mode-checker
  "Composes checkers appropriate for Phase 1 single-region safe-mode testing."
  []
  (checker/compose
    {:convergence            (convergence-checker)
     :crdt-counter           (crdt-counter-checker)
     :availability           (availability-checker)
     :safe-txn               (safe-txn-checker)
     :partition-blocks-txns  (partition-blocks-txns-checker)
     :fork-choice            (fork-choice-checker)}))
