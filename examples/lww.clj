(ns clj-3df.examples.lww
  (:require
   [clj-3df.core :refer [create-db register-query plan-rules]]))

;; LWW Register
;; https://speakerdeck.com/ept/data-structures-as-queries-expressing-crdts-using-datalog?slide=15

(def schema
  {:assign/time  {:db/valueType :Number}
   :assign/key   {:db/valueType :Number}
   :assign/value {:db/valueType :Number}})

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

(compile-query db q)
