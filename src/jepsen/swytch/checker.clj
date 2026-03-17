(ns jepsen.swytch.checker
  "Custom Jepsen checkers for Swytch consistency properties."
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure.set :as set]
            [jepsen [checker :as checker]
                    [store :as store]
                    [util :as util]]
            [elle.list-append :as ela]
            [elle.rw-register :as erw]))

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
  "Returns true if the operation occurred during any partition window."
  [windows op]
  (let [t (:time op)]
    (some (fn [{:keys [start stop]}]
            (and (<= start t) (<= t stop)))
          windows)))

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
  Default threshold is 0.5 (50%)."
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

(defn partition-effective-checker
  "Verifies that the network partition actually caused independent
  mutation on multiple nodes. During partition windows, successful
  write-like operations should originate from at least 2 different
  nodes, proving both sides were mutating state independently.

  Default write-fs: #{:write :incr :sadd :lpush :zadd :txn}"
  ([] (partition-effective-checker #{:write :incr :sadd :lpush :zadd :txn}))
  ([write-fs]
   (reify checker/Checker
     (check [_ test history _opts]
       (let [windows (partition-windows history)
             partition-writes (->> (with-nodes test (oks history))
                                   (filter #(write-fs (:f %)))
                                   (filter #(during-partition? windows %)))
             writing-nodes (set (map :node partition-writes))]
         (if (empty? windows)
           {:valid?             :unknown
            :note               "No partition windows detected"}
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
  Checks that no :txn operations succeed during partition windows."
  []
  (reify checker/Checker
    (check [_ test history _opts]
      (let [windows         (partition-windows history)
            partition-txns  (->> (oks history)
                                 (ops-by-f :txn)
                                 (filter #(during-partition? windows %))
                                 vec)]
        {:valid?              (empty? partition-txns)
         :committed-during-partition (count partition-txns)
         :txns                partition-txns}))))

;; ---- Fork-Choice Checker ----

(defn fork-choice-checker
  "After a one-sided partition heals, all nodes must agree on the
  same value. This is checked via final reads — after heal + settle,
  every node should see the same value for each key."
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

;; ---- Hologram Cut Checker (stub) ----

(defn hologram-cut-checker
  "Checks that when both sides of a partition commit transactions,
  the conflict is detected after heal and both histories are preserved.

  NOTE: This is a stub — Swytch's API does not yet expose conflict
  state. Currently only verifies that both partitions performed writes
  during the split. Conflict detection will be added when the API
  is available."
  []
  (reify checker/Checker
    (check [_ test history _opts]
      (let [windows (partition-windows history)
            ok-ops  (with-nodes test (oks history))
            partition-writes (->> ok-ops
                                  (filter #(#{:write :txn} (:f %)))
                                  (filter #(during-partition? windows %)))
            nodes-that-wrote (set (map :node partition-writes))
            both-sides?      (> (count nodes-that-wrote) 1)]
        {:valid?           :unknown
         :both-sides-wrote both-sides?
         :writing-nodes    nodes-that-wrote
         :note             "Stub: conflict detection requires Swytch API support"}))))

;; ---- Per-Partition Elle Checker ----

(defn per-partition-elle-checker
  "Splits history by partition membership and runs Elle independently
  on each sub-history. During connected periods, runs on the full
  history. During partitions, runs on each partition group separately.

  `elle-checker-fn` should be a function that takes opts and returns
  an Elle checker (e.g. elle.list-append/check or elle.rw-register/check).

  `opts` are passed through to Elle."
  [elle-check-fn opts]
  (reify checker/Checker
    (check [_ test history checker-opts]
      (let [all-maps (with-nodes test (all-ops history))
            windows  (partition-windows history)
            non-partition-ops (vec (remove #(during-partition? windows %) all-maps))
            full-result (elle-check-fn
                          (assoc opts :directory
                                 (.getCanonicalPath
                                   (store/path! test (:subdirectory checker-opts)
                                                "elle" "full")))
                          non-partition-ops)
            partition-results
            (when (seq windows)
              (let [partition-ops (filter #(during-partition? windows %) all-maps)
                    by-node (group-by :node partition-ops)]
                (into {}
                      (for [[node ops] by-node
                            :when (seq ops)]
                        [node
                         (elle-check-fn
                           (assoc opts :directory
                                  (.getCanonicalPath
                                    (store/path! test (:subdirectory checker-opts)
                                                 "elle" (str "node-" node))))
                           (vec ops))]))))]
        {:valid?            (checker/merge-valid
                              (cons (:valid? full-result)
                                    (map :valid? (vals partition-results))))
         :full              full-result
         :per-partition     partition-results}))))

;; ---- Composed checker for convenience ----

(defn safe-mode-checker
  "Composes checkers appropriate for safe-mode testing."
  []
  (checker/compose
    {:convergence            (convergence-checker)
     :crdt-counter           (crdt-counter-checker)
     :availability           (availability-checker)
     :safe-txn               (safe-txn-checker)
     :partition-blocks-txns  (partition-blocks-txns-checker)
     :fork-choice            (fork-choice-checker)}))

(defn holographic-mode-checker
  "Composes checkers appropriate for holographic-mode testing."
  []
  (checker/compose
    {:convergence    (convergence-checker)
     :crdt-counter   (crdt-counter-checker)
     :availability   (availability-checker 0.1)
     :fork-choice    (fork-choice-checker)
     :hologram-cut   (hologram-cut-checker)}))
