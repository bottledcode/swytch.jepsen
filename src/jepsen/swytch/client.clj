(ns jepsen.swytch.client
  "Jepsen client for Swytch over the Redis protocol using Carmine."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [client :as client]
                    [util :as util]]
            [jepsen.swytch.db :as sdb]
            [taoensso.carmine :as car])
  (:import [java.net ConnectException SocketException]
           [java.io IOException]))

(defn conn-spec
  "Returns a Carmine connection spec for the given node."
  [node]
  {:pool {}
   :spec {:host node
          :port sdb/redis-port}})

(defmacro with-errors
  "Catches Redis/connection exceptions and maps them to Jepsen :info
  (indeterminate) or :fail results as appropriate."
  [op & body]
  `(try ~@body
        (catch clojure.lang.ExceptionInfo e#
          (let [data# (ex-data e#)]
            (if (re-find #"READONLY|LOADING|CLUSTERDOWN" (str (.getMessage e#)))
              (assoc ~op :type :fail :error (.getMessage e#))
              (assoc ~op :type :info :error (.getMessage e#)))))
        (catch ConnectException e#
          (assoc ~op :type :fail :error :connection-refused))
        (catch SocketException e#
          (assoc ~op :type :info :error (.getMessage e#)))
        (catch IOException e#
          (assoc ~op :type :info :error (.getMessage e#)))
        (catch java.util.concurrent.TimeoutException e#
          (assoc ~op :type :info :error :timeout))))

(defrecord SwytchClient [conn node]
  client/Client
  (open! [this test node]
    (assoc this
           :conn (conn-spec node)
           :node node))

  (setup! [this test])

  (invoke! [this test op]
    (with-errors op
      (case (:f op)
        ;; String operations
        :read    (let [v (car/wcar conn (car/get (str (:key op))))]
                   (if (instance? Exception v)
                     (assoc op :type :fail :error (.getMessage ^Exception v))
                     (assoc op :type :ok :value (when v (parse-long v)))))
        :write   (let [v (car/wcar conn (car/set (str (:key op)) (str (:value op))))]
                   (if (instance? Exception v)
                     (assoc op :type :fail :error (.getMessage ^Exception v))
                     (assoc op :type :ok)))
        :incr    (let [v (car/wcar conn (car/incr (str (:key op))))]
                   (if (instance? Exception v)
                     (assoc op :type :fail :error (.getMessage ^Exception v))
                     (assoc op :type :ok :value v)))

        ;; Set operations
        :sadd    (let [v (car/wcar conn (apply car/sadd (str (:key op)) (:value op)))]
                   (if (instance? Exception v)
                     (assoc op :type :fail :error (.getMessage ^Exception v))
                     (assoc op :type :ok :value v)))
        :smembers (let [v (car/wcar conn (car/smembers (str (:key op))))]
                    (if (instance? Exception v)
                      (assoc op :type :fail :error (.getMessage ^Exception v))
                      (assoc op :type :ok :value (set v))))

        ;; Sorted set operations
        :zadd    (let [{:keys [key score member]} op
                       v (car/wcar conn (car/zadd (str key) score member))]
                   (if (instance? Exception v)
                     (assoc op :type :fail :error (.getMessage ^Exception v))
                     (assoc op :type :ok :value v)))
        :zrange  (let [v (car/wcar conn (car/zrange (str (:key op)) 0 -1))]
                   (if (instance? Exception v)
                     (assoc op :type :fail :error (.getMessage ^Exception v))
                     (assoc op :type :ok :value (vec v))))

        ;; List operations
        :lpush   (let [v (car/wcar conn (apply car/lpush (str (:key op)) (:value op)))]
                   (if (instance? Exception v)
                     (assoc op :type :fail :error (.getMessage ^Exception v))
                     (assoc op :type :ok :value v)))
        :lrange  (let [v (car/wcar conn (car/lrange (str (:key op)) 0 -1))]
                   (if (instance? Exception v)
                     (assoc op :type :fail :error (.getMessage ^Exception v))
                     (assoc op :type :ok :value (vec v))))

        ;; Transaction (WATCH/MULTI/EXEC)
        :txn     (let [txn-ops (:value op)
                       results
                       (car/wcar conn
                         (let [keys (distinct (map (comp str :key) txn-ops))]
                           (doseq [k keys] (car/watch k)))
                         (car/multi)
                         (doseq [{:keys [f key value]} txn-ops]
                           (case f
                             :read  (car/get (str key))
                             :write (car/set (str key) (str value))
                             :incr  (car/incr (str key))))
                         (car/exec))
                       exec-result (last results)]
                   (cond
                     (nil? exec-result)
                     (assoc op :type :fail :error :watch-conflict)

                     (some #(instance? Exception %) exec-result)
                     (assoc op :type :fail
                               :error (mapv #(if (instance? Exception %)
                                               (.getMessage %)
                                               %)
                                            exec-result))

                     :else
                     (assoc op :type :ok :value exec-result))))))

  (teardown! [this test])

  (close! [this test]))

(defn client
  "Constructs an unconnected SwytchClient."
  []
  (map->SwytchClient {}))
