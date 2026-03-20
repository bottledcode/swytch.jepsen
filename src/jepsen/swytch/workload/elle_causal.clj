(ns jepsen.swytch.workload.elle-causal
  "Elle list-append causal consistency workload for safe mode.

  Uses Elle's transactional dependency analysis to verify light cone
  consistency guarantees:
    - Causal ordering: if e causally precedes e', no node sees them out of order
    - Causal closure: cannot observe an effect without its causal history
    - Read-your-writes: single connection always reflects prior writes
    - Monotonic reads: no client goes backward in causal time

  Maps Elle's list-append model to Redis:
    - [:append k v] -> WATCH/MULTI/RPUSH/EXEC (serialized via optimistic locking)
    - [:r k nil]    -> LRANGE
    - Multi-op txns -> WATCH/MULTI/EXEC"
  (:require [clojure.tools.logging :refer [info warn]]
            [elle.list-append :as ella]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [store :as store]]
            [jepsen.swytch.db :as sdb]
            [taoensso.carmine :as car])
  (:import [java.net ConnectException SocketException]
           [java.io IOException]))

;; ---- Key helpers ----

(def key-prefix "el-")

(defn elle-key
  "Converts an Elle integer key to a Redis string key."
  [k]
  (str key-prefix k))

(defn parse-list
  "Parses a Redis list response (vector of strings) into a vector of longs.
  Returns an empty vector for nil or empty responses."
  [vs]
  (if (or (nil? vs) (instance? Exception vs))
    (when (instance? Exception vs)
      (throw (ex-info (.getMessage ^Exception vs) {})))
    (mapv #(Long/parseLong (str %)) vs)))

;; ---- Error handling ----

(defmacro with-errors
  "Catches Redis/connection exceptions and maps them to Jepsen result types."
  [op & body]
  `(try ~@body
        (catch clojure.lang.ExceptionInfo e#
          (let [msg# (str (.getMessage e#))]
            (if (re-find #"READONLY|LOADING|CLUSTERDOWN" msg#)
              (assoc ~op :type :fail :error msg#)
              (assoc ~op :type :info :error msg#))))
        (catch ConnectException _#
          (assoc ~op :type :fail :error :connection-refused))
        (catch SocketException e#
          (assoc ~op :type :info :error (.getMessage e#)))
        (catch IOException e#
          (assoc ~op :type :info :error (.getMessage e#)))
        (catch java.util.concurrent.TimeoutException _#
          (assoc ~op :type :info :error :timeout))))

;; ---- Transaction execution ----

(defn throw-if-exception
  "If x is an Exception, throw it wrapped in ex-info. Otherwise return x."
  [x]
  (if (instance? Exception x)
    (throw (ex-info (.getMessage ^Exception x) {:cause x}))
    x))

(defn exec-single-read
  "Executes a single read micro-op without transaction overhead."
  [conn [_ k _]]
  (let [result (throw-if-exception
                 (car/wcar conn (car/lrange (elle-key k) 0 -1)))]
    [[:r k (parse-list result)]]))

(defn exec-single-append
  "Executes a single append micro-op using WATCH/MULTI/EXEC to serialize
  concurrent appends and maintain a total ordering of list elements."
  [conn [_ k v]]
  (let [rk      (elle-key k)
        results (throw-if-exception
                  (car/wcar conn
                    (car/watch rk)
                    (car/multi)
                    (car/rpush rk (str v))
                    (car/exec)))
        exec-result (last results)]
    (when exec-result
      (let [exec-result (throw-if-exception exec-result)]
        (when (sequential? exec-result)
          (when (some #(instance? Exception %) exec-result)
            (throw (ex-info (str "Append error: "
                                 (pr-str (filterv #(instance? Exception %) exec-result)))
                            {})))
          [[:append k v]])))))

(defn exec-multi-op-txn
  "Executes a multi-op transaction using WATCH/MULTI/EXEC.
  Returns completed micro-ops with reads filled in, or nil on WATCH conflict."
  [conn txn]
  (let [keys    (distinct (map (fn [[_ k]] (elle-key k)) txn))
        results (throw-if-exception
                  (car/wcar conn
                    (doseq [k keys] (car/watch k))
                    (car/multi)
                    (doseq [[f k v] txn]
                      (case f
                        :r      (car/lrange (elle-key k) 0 -1)
                        :append (car/rpush (elle-key k) (str v))))
                    (car/exec)))
        exec-result (last results)]
    (when exec-result
      (let [exec-result (throw-if-exception exec-result)]
        (when (sequential? exec-result)
          (when (some #(instance? Exception %) exec-result)
            (throw (ex-info (str "Transaction error: "
                                 (pr-str (filterv #(instance? Exception %) exec-result)))
                            {})))
          (mapv (fn [[f k v] result]
                  (case f
                    :r      [:r k (parse-list result)]
                    :append [:append k v]))
                txn exec-result))))))

(defn exec-txn
  "Executes an Elle transaction against Redis.
  Dispatches to optimized paths for single-op transactions."
  [conn txn]
  (if (= 1 (count txn))
    (let [[f _ _] (first txn)]
      (case f
        :r      (exec-single-read conn (first txn))
        :append (exec-single-append conn (first txn))))
    (exec-multi-op-txn conn txn)))

;; ---- Elle client ----

(defrecord ElleClient [conn node]
  client/Client
  (open! [this _test node]
    (assoc this
           :conn {:pool {} :spec {:host node :port sdb/redis-port}}
           :node node))

  (setup! [this _test])

  (invoke! [this _test op]
    (with-errors op
      (case (:f op)
        :txn (let [result (exec-txn conn (:value op))]
               (if result
                 (assoc op :type :ok :value result)
                 (assoc op :type :fail :error :watch-conflict))))))

  (teardown! [this _test])

  (close! [this _test]))

(defn elle-client []
  (map->ElleClient {}))

;; ---- Elle checker wrapper ----

(defn elle-checker
  "Wraps Elle's list-append check function as a Jepsen Checker."
  [opts]
  (reify checker/Checker
    (check [_ test history _checker-opts]
      (let [dir (store/path! test "elle")]
        (ella/check (assoc opts :directory (.getCanonicalPath dir))
                    history)))))

;; ---- Workload ----

(def key-count
  "Number of distinct keys to use."
  5)

(def max-txn-length
  "Maximum number of micro-ops per transaction."
  4)

(let [next-val (atom 0)]
  (defn txn-op
    "Generates a random Elle list-append transaction with globally unique
    append values. Each key is appended to at most once per transaction
    (Elle requires unique appends per key across the entire history)."
    [_ _]
    (let [n-ops       (inc (rand-int max-txn-length))
          appended    (volatile! #{})]
      {:type  :invoke
       :f     :txn
       :value (vec (repeatedly n-ops
                     (fn []
                       (let [k (rand-int key-count)]
                         (if (or (< (rand) 0.5) (@appended k))
                           [:r k nil]
                           (do (vswap! appended conj k)
                               [:append k (swap! next-val inc)]))))))})))

(defn final-reads
  "Generator that reads all keys from each thread, giving Elle
  a complete view of the final state."
  []
  (gen/each-thread
    (gen/once
      (fn [_ _]
        {:type :invoke, :f :txn,
         :value (mapv (fn [k] [:r k nil]) (range key-count))}))))

(defn workload
  "Returns a workload map for Elle causal consistency testing.

  Options:
    :rate  - ops per second (default 50)"
  [{:keys [rate] :or {rate 50}}]
  {:client          (elle-client)
   :generator       (->> txn-op
                         (gen/stagger (/ 1 rate)))
   :final-generator (final-reads)
   :checker         (elle-checker {:consistency-models [:causal-cerone]})})
