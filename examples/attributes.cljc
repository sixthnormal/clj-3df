(ns attributes
  (:require
   [clj-3df.core :as df :use [exec!]]))

(def schema
  {:device/speed-one  {:db/valueType :Number
                       :db/semantics :db.semantics.cardinality/one}
   :device/speed-many {:db/valueType :Number
                       :db/semantics :db.semantics.cardinality/many}})

(def db (df/create-db schema))

(comment

  (do
    (def conn (df/create-debug-conn! "ws://127.0.0.1:6262"))
    (exec! conn (df/create-db-inputs db)))

  (exec! conn
    (df/transact db [[:db/add 100 :device/speed-one 1]
                     [:db/add 100 :device/speed-many 1]]))

  (exec! conn
    (df/query
     db "speed-one"
     '[:find ?device ?speed 
       :where [?device :device/speed-one ?speed]])
    (df/query
     db "speed-many"
     '[:find ?device ?speed 
       :where [?device :device/speed-many ?speed]]))

  (exec! conn
    (df/transact db [[:db/add 100 :device/speed-one 2]
                     [:db/add 100 :device/speed-many 2]]))

  (exec! conn
    (df/transact db [[:db/add 100 :device/speed-one 3]
                     [:db/add 100 :device/speed-one 4]
                     [:db/add 100 :device/speed-one 5]
                     [:db/add 100 :device/speed-many 3]
                     [:db/add 100 :device/speed-many 4]
                     [:db/add 100 :device/speed-many 5]]))

  )
