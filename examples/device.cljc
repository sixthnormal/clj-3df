(ns device
  (:require
   [clj-3df.core :as df :use [exec!]]))


(def schema
  {:settings/speed {:db/valueType :Number}
   :device/speed   {:db/valueType :Number}})

(def db (df/create-db schema))



(comment

  (do
    (def conn (df/create-debug-conn "ws://127.0.0.1:6262"))
    (exec! conn (df/create-db-inputs db)))

  (def t0 0)
  (def t1 1)
  (def t2 2)

  (exec! conn
    (df/transact db t0 [[:db/add 111 :settings/speed 100]]))

  (exec! conn
    (df/register-query
     db "current-speed"
     '[:find ?device ?speed 
       :where [?device :device/speed ?speed]]))

  (exec! conn
    (df/register-query
     db "current-settings"
     '[:find ?device ?speed 
       :where [?device :settings/speed ?speed]]))

  (exec! conn
    (df/register-query
     db "deviating"
     '[:find ?device ?deviation
       :where
       [?device :settings/speed ?target]
       [?device :device/speed ?speed]
       [(< ?speed ?target)]
       [(subtract ?target ?speed) ?deviation]]))

  ;; send inputs without closing t1 epoch yet
  
  (exec-raw!
   conn
   [{:Datom [111 :device/speed {:Number 50} 1 t1]}])

  ;; still not done with t1...
  
  (exec-raw!
   conn
   [{:Datom [111 :device/speed {:Number 50} -1 t1]}
    {:Datom [111 :device/speed {:Number 75} 1 t1]}])

  ;; nothing in flight, advance to t2...
  
  (exec-raw! conn [{:AdvanceInput [nil t2]}])
  )
