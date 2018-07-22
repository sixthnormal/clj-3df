(ns clj-3df.examples.lww
  (:require
   [clj-3df.core :refer [create-conn create-db exec!
                         register-plan register-query transact]]
   [manifold.stream :as stream]
   [manifold.bus :as bus]))

;; LWW Register
;; https://speakerdeck.com/ept/data-structures-as-queries-expressing-crdts-using-datalog?slide=15

(def schema
  {:assign/time  {:db/valueType :Number}
   :assign/key   {:db/valueType :Number}
   :assign/value {:db/valueType :String}})

(def db (create-db schema))

(def rules
  '[[(older? ?t1 ?key)
     [?op :assign/key ?key] [?op :assign/time ?t1]
     [?op2 :assign/key ?key] [?op2 :assign/time ?t2]
     [(< ?t1 ?t2)]]

    [(lww ?t ?key ?val)
     [?op :assign/time ?t]
     [?op :assign/key ?key]
     [?op :assign/value ?val]
     (not (older? ?t ?key))]])

;; possible ergonomics improvement

(def rules'
  '[[(older? ?t1 ?key)
     #:assign{:key ?key :time ?t1}
     #:assign{:key ?key :time ?t2}
     [(< ?t1 ?t2)]]

    [(lww ?key ?value)
     #:assign{:time ?t :key ?key :value ?value}
     (not (older ?t ?key))]])

;; query

(def q '[:find ?k ?v :where (lww ?k ?v)])

;; test

(comment
  
  (def conn (create-conn "ws://127.0.0.1:6262"))
  (stream/consume #(println %) (bus/subscribe (:out conn) :out))

  (exec! conn (register-query db "lww_crdt" q rules))

  (exec! conn
    (transact db [{:db/id 1 :assign/time 4 :assign/key 100 :assign/value "X"}])
    (expect-> out (assert (= [[[{"Number" 4} {"Number" 100} {"String" "X"}] 1]] out))))

  (exec! conn
    (transact db [{:db/id 2 :assign/time 2 :assign/key 100 :assign/value "Y"}]))

  (exec! conn
    (transact db [{:db/id 4 :assign/time 10 :assign/key 100 :assign/value "Z"}
                  {:db/id 5 :assign/time 10 :assign/key 200 :assign/value "Z"}])
    (expect-> out (assert (= [[[{"Number" 4} {"Number" 100} {"String" "X"}] -1]
                              [[{"Number" 10} {"Number" 100} {"String" "Z"}] 1]
                              [[{"Number" 10} {"Number" 200} {"String" "Z"}] 1]] out))))

  (exec! conn
    (transact db [{:db/id 3 :assign/time 6 :assign/key 200 :assign/value "Y"}])

    (transact db [{:db/id 3 :assign/time 1 :assign/key 200 :assign/value "Y"}]))
  )
