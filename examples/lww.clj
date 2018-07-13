(ns clj-3df.examples.lww
  (:require
   [clj-3df.core :refer [create-conn create-db exec!
                         register-plan register-query
                         plan-rules transact]]))

;; LWW Register
;; https://speakerdeck.com/ept/data-structures-as-queries-expressing-crdts-using-datalog?slide=15

(def schema
  {:assign/time  {:db/valueType :Number}
   :assign/key   {:db/valueType :Number}
   :assign/value {:db/valueType :String}})

(def db (create-db schema))

(def rules
  '[[(older ?t1 ?key)
     [?op :assign/key ?key] [?op :assign/time ?t1]
     [?op2 :assign/key ?key] [?op2 :assign/time ?t2]
     [(< ?t1 ?t2)]]

    [(lww ?key ?val)
     [?op :assign/time ?t]
     [?op :assign/key ?key]
     [?op :assign/value ?val]
     (not (older ?t ?key))]])

(plan-rules db rules)

;; possible ergonomics improvement

(def rules'
  '[[(older ?t1 ?key)
     #:assign{:key ?key :time ?t1}
     #:assign{:key ?key :time ?t2}
     [(< ?t1 ?t2)]]

    [(lww ?key ?value)
     #:assign{:time ?t :key ?key :value ?value}
     (not (older ?t ?key))]])

(plan-rules db rules')

;; query

(def q '[:find ?k ?v :where (lww ?k ?v)])

;; test

(def conn (create-conn "ws://127.0.0.1:6262"))

(plan-rules db rules)

(exec! conn
  (register-query db "lww" '[:find ?op ?op2 ?key ?t ?t
                             :where
                             [?op :assign/time ?t]
                             [?op :assign/key ?key]
                             [?op2 :assign/time ?t2]
                             [?op2 :assign/key ?key]
                             [(< ?t2 ?t)]] []))

(exec! conn
  (transact db [{:db/id 1 :assign/time 4 :assign/key 100 :assign/value "X"}
                {:db/id 1 :assign/time 2 :assign/key 100 :assign/value "Y"}]))

(exec! conn
  (transact db [{:db/id 2 :assign/time 4 :assign/key 100 :assign/value "X"}
                {:db/id 2 :assign/time 6 :assign/key 100 :assign/value "Y"}]))

(exec! conn
  (transact db [{:db/id 3 :assign/time 5 :assign/key 100 :assign/value "X"}
                {:db/id 3 :assign/time 5 :assign/key 100 :assign/value "Y"}]))
