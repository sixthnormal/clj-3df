(ns clj-3df.core
  (:refer-clojure :exclude [resolve])
  (:require
   [clojure.string :as str]
   [clojure.set :as set]
   #?(:clj [clojure.spec.alpha :as s]
      :cljs [cljs.spec.alpha :as s])
   [aleph.http :as http]
   [manifold.stream :as stream]
   [cheshire.core :as json]
   [clj-3df.parser :as parser]))

(def ^{:arglists '([db query])} plan-query parser/plan-query)
(def ^{:arglists '([db rules])} plan-rules parser/plan-rules)

(defrecord Differential [schema attr->int int->attr next-tx impl registrations])

(defn create-db [schema]
  (let [attr->int (zipmap (keys schema) (iterate (partial + 100) 100))
        int->attr (set/map-invert attr->int)]
    (Differential. schema attr->int int->attr 0 nil {})))

(defn register-query [db name query]
  {:Register {:query_name name
              :plan       (plan-query db query)
              :rules      []}})

(defn register-query! [conn db name query]
  (->> (register-query db name query) (json/generate-string) (stream/put! conn)))

(defn transact [tx-data]
  {:Transact {:tx_data tx-data}})

(defn transact! [conn tx-data]
  (->> (transact tx-data) (json/generate-string) (stream/put! conn)))

(def conn @(http/websocket-client "ws://127.0.0.1:6262"))

(def subscriber
  (Thread.
   (fn []
     (println "[SUBSCRIBER] running")
     (loop []
       (when-let [result @(stream/take! conn ::drained)]
         (if (= result ::drained)
           (println "[SUBSCRIBER] server closed connection")
           (do
             (println result)
             (recur))))))))

(comment
  (.start subscriber)
  (.getState subscriber)

  (def schema
    {:name   {:db/valueType :String}
     :age    {:db/valueType :Number}
     :friend {:db/valueType :Eid}
     :edge   {:db/valueType :Eid}
     :admin? {:db/valueType :Bool}})

  (def db (create-db schema))

  (register-query! conn db "test" '[:find ?e :where [?e :name "Mabel"]])

  (transact! conn [[1, 1, 100, {:String "Dipper"}],
                   [1, 2, 100, {:String "Mabel"}]]))
