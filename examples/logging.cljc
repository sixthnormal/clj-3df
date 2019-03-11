(ns clj-3df.logging
  (:require
   [clj-3df.core :as df :refer [exec!]]
   [clj-3df.binding :as binding]))

(def schema
  {:timely/scope {:db/valueType :Eid}

   :timely.event.operates/local-id     {:db/valueType :Eid}
   :timely.event.operates/name         {:db/valueType :String}
   :timely.event.operates/address      {:db/valueType :Address}
   :timely.event.channels/src-index    {:db/valueType :Eid}
   :timely.event.channels/src-port     {:db/valueType :Eid}
   :timely.event.channels/target-index {:db/valueType :Eid}
   :timely.event.channels/target-port  {:db/valueType :Eid}

   :differential.event/size {:db/valueType :Number}})

(defn timely? [kw]
  (clojure.string/starts-with? (namespace kw) "timely"))

(defn differential? [kw]
  (clojure.string/starts-with? (namespace kw) "differential"))

(def db (df/create-db schema))

(comment

  (def conn (df/create-debug-conn! "ws://127.0.0.1:6262"))

  (exec! conn
    (df/register-source
     {:TimelyLogging
      {:attributes (into [] (filter timely?) (keys schema))}}))

  (exec! conn
    (df/register-source
     {:DifferentialLogging
      {:attributes (into [] (filter differential?) (keys schema))}}))

  (exec! conn
    (df/query
     db "timely/channels"
     '[:find ?x ?scope ?src-index ?src-port ?target-index ?target-port
       :where
       [?x :timely/scope ?scope]
       [?x :timely.event.channels/src-index ?src-index]
       [?x :timely.event.channels/src-port ?src-port]
       [?x :timely.event.channels/target-index ?target-index]
       [?x :timely.event.channels/target-port ?target-port]]))

  (exec! conn
    (df/query
     db "timely/graph"
     '[:find ?x ?name ?id ?scope
       :where
       [?x :timely/scope ?scope]
       [?x :timely.event.operates/local-id ?id]
       [?x :timely.event.operates/name ?name]]))

  (exec! conn
    (df/uninterest "timely/graph"))
  
  )
