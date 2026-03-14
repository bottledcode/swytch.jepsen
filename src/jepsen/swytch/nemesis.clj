(ns jepsen.swytch.nemesis
  "Nemesis configurations for Swytch Jepsen tests. Leverages Jepsen's
  built-in nemesis packages (partitions, kill/pause, clock, packet)
  and adds Swytch-specific partition strategies (region-based split,
  asymmetric partition).

  Exposes two composed nemesis packages:
    - `safe-nemesis`        — for safe-mode testing
    - `holographic-nemesis` — for holographic-mode testing

  Both use phased scheduling: normal → fault → heal → settle → final-read."
  (:require [clojure.set :as set]
            [clojure.tools.logging :refer [info]]
            [jepsen [generator :as gen]
                    [nemesis :as n]
                    [net :as net]
                    [random :as rand]]
            [jepsen.nemesis.combined :as nc]
            [jepsen.swytch.cluster-config :as cc]))

;; ---- Custom partition grudges ----

(defn region-grudge
  "Splits nodes by their region assignment (region-a vs region-b).
  Nodes in the same region can communicate; cross-region is blocked."
  [test]
  (let [nodes  (:nodes test)
        groups (group-by #(cc/node-region test %) nodes)]
    (n/complete-grudge (vals groups))))

(defn island-grudge
  "Isolates every node from every other node — N partitions of 1."
  [test]
  (n/complete-grudge (map vector (:nodes test))))

(defn asymmetric-grudge
  "Creates an asymmetric partition: splits nodes into three groups
  A, B, C where A↔B can communicate, B↔C can communicate, but
  A↔C is blocked. B acts as a bridge-like group but neither A nor C
  can reach the other."
  [test]
  (let [nodes   (rand/shuffle (:nodes test))
        n       (count nodes)
        _       (when (< n 3)
                  (throw (ex-info "Asymmetric partition requires at least 3 nodes"
                                  {:node-count n})))
        ;; Split into thirds, ensuring each group has at least 1 node
        a-size  (max 1 (quot n 3))
        b-size  (max 1 (quot n 3))
        c-size  (- n a-size b-size)
        ;; If c would be empty, shrink b to make room
        b-size  (if (pos? c-size) b-size (max 1 (- n a-size 1)))
        a       (set (take a-size nodes))
        b       (set (take b-size (drop a-size nodes)))
        c       (set (drop (+ a-size b-size) nodes))]
    ;; A hates C, C hates A; B is friendly with both
    (merge
      (into {} (for [node a] [node c]))
      (into {} (for [node c] [node a])))))

;; ---- Custom partition specs for combined nemesis ----

(defn region-partition-spec
  "A partition spec that splits by Swytch region assignment."
  [test _db]
  (region-grudge test))

(defn island-partition-spec
  "A partition spec that isolates every node."
  [test _db]
  (island-grudge test))

(defn asymmetric-partition-spec
  "A partition spec for asymmetric A↔C blocked, A↔B and B↔C ok."
  [test _db]
  (asymmetric-grudge test))

;; ---- Partition nemesis with custom specs ----

(defn swytch-partition-nemesis
  "A partition nemesis that supports both standard Jepsen specs and
  Swytch-specific ones (:region, :island, :asymmetric)."
  [db]
  (let [p (n/partitioner)]
    (reify
      n/Reflection
      (fs [_] #{:start-partition :stop-partition})

      n/Nemesis
      (setup! [this test]
        (n/setup! p test)
        this)

      (invoke! [_ test op]
        (-> (case (:f op)
              :start-partition
              (let [grudge (case (:value op)
                             :region    (region-grudge test)
                             :island    (island-grudge test)
                             :asymmetric (asymmetric-grudge test)
                             ;; Fall back to combined.clj's grudge for standard specs
                             (nc/grudge test db (:value op)))]
                (n/invoke! p test (assoc op :f :start :value grudge)))
              :stop-partition
              (n/invoke! p test (assoc op :f :stop)))
            (assoc :f (:f op))))

      (teardown! [_ test]
        (n/teardown! p test)))))

(defn swytch-partition-package
  "Like Jepsen's partition-package but includes Swytch-specific partition types."
  [opts]
  (let [needed?  ((:faults opts) :partition)
        targets  (:targets (:partition opts)
                           [:one :majority :majorities-ring
                            :region :island :asymmetric])
        start    (fn [_ _] {:type :info
                            :f :start-partition
                            :value (rand/nth targets)})
        stop     {:type :info :f :stop-partition :value nil}
        gen      (->> (gen/flip-flop start (gen/repeat stop))
                      (gen/stagger (:interval opts nc/default-interval)))]
    {:generator       (when needed? gen)
     :final-generator (when needed? stop)
     :nemesis         (swytch-partition-nemesis (:db opts))
     :perf            #{{:name  "partition"
                         :start #{:start-partition}
                         :stop  #{:stop-partition}
                         :color "#E9DCA0"}}}))

;; ---- Composed nemesis packages ----

(defn swytch-nemesis-packages
  "Returns a collection of nemesis packages for the given opts.
  Uses Swytch's custom partition nemesis instead of the default."
  [opts]
  (let [faults (set (:faults opts))
        opts   (assoc opts :faults faults)]
    [(swytch-partition-package opts)
     (nc/packet-package opts)
     (nc/clock-package opts)
     (nc/db-package opts)]))

(defn nemesis-package
  "Composes all nemesis packages into one. Options:

    :db         The SwytchDB instance
    :faults     Set of enabled faults, e.g. #{:partition :kill :pause :clock :packet}
    :interval   Seconds between nemesis operations (default 10)
    :partition  {:targets [...]}  — partition specs to use
    :packet     {:targets [...] :behaviors [...]}
    :kill       {:targets [...]}
    :pause      {:targets [...]}
    :clock      {:targets [...]}"
  [opts]
  (nc/compose-packages (swytch-nemesis-packages opts)))

;; ---- Phased generator helpers ----

(defn phase-generator
  "Returns a generator that runs through phases:
    1. Normal operation (no faults) for `normal-secs`
    2. Fault injection for `fault-secs`
    3. Heal all faults
    4. Settle for `settle-secs` (normal ops, no faults)
    5. Final reads

  `client-gen` is the generator for client operations.
  `nemesis-pkg` is a nemesis package from `nemesis-package`."
  [{:keys [normal-secs fault-secs settle-secs]
    :or   {normal-secs 10
           fault-secs  30
           settle-secs 10}}
   nemesis-pkg client-gen final-gen]
  (gen/phases
    ;; Phase 1: normal operation
    (gen/clients
      (gen/time-limit normal-secs client-gen))

    ;; Phase 2: faults + client ops
    (->> client-gen
         (gen/nemesis (:generator nemesis-pkg))
         (gen/time-limit fault-secs))

    ;; Phase 3: heal
    (gen/nemesis (:final-generator nemesis-pkg))

    ;; Phase 4: settle
    (gen/clients
      (gen/time-limit settle-secs client-gen))

    ;; Phase 5: final reads
    (when final-gen
      (gen/clients final-gen))))

;; ---- Pre-built nemesis configurations ----

(defn safe-nemesis
  "Nemesis package for safe-mode testing. Includes:
    - Region-based partitions (tests light-cone behavior)
    - Node kill/restart (tests effects log recovery)
    - Clock skew (tests HLC absorption)"
  [db]
  (nemesis-package
    {:db        db
     :faults    #{:partition :kill :clock}
     :partition {:targets [:region :majority]}
     :kill      {:targets [:one :minority]}
     :clock     {:targets [:one]}}))

(defn holographic-nemesis
  "Nemesis package for holographic-mode testing. Includes:
    - All partition types including island and asymmetric
    - Node kill/restart and pause/resume
    - Clock skew
    - Packet delay (stresses horizon wait)"
  [db]
  (nemesis-package
    {:db        db
     :faults    #{:partition :kill :pause :clock :packet}
     :partition {:targets [:region :island :asymmetric :majority :majorities-ring]}
     :kill      {:targets [:one :minority]}
     :pause     {:targets [:one]}
     :clock     {:targets [:one :minority]}
     :packet    {:targets   [:one :minority]
                 :behaviors [{:delay {}}]}}))
