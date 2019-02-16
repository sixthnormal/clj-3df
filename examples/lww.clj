(ns lww
  (:require
   [clojure.pprint :as pprint]
   [clj-3df.core :refer [create-conn create-db exec!
                         register-plan query transact] :as df])
  (:gen-class))

;; LWW Register
;; Datalog version of a last-write-wins register,
;; translated from Martin Kleppman's work:
;;
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

(defn -main []
  (def conn (df/create-debug-conn "ws://127.0.0.1:6262"))

  (exec! conn (df/create-db-inputs db))

  (exec! conn (query db "lww_crdt" q rules))

  (exec! conn
    (transact db [{:db/id 1 :assign/time 4 :assign/key 100 :assign/value "X"}])
    #_(expect-> out (assert (= ["lww_crdt" [[[4 100 "X"] 0 1]]] out))))

  (exec! conn
    (transact db [{:db/id 2 :assign/time 2 :assign/key 100 :assign/value "Y"}]))

  (exec! conn
    (transact db [{:db/id 4 :assign/time 10 :assign/key 100 :assign/value "Z"}
                  {:db/id 5 :assign/time 10 :assign/key 200 :assign/value "Z"}])
    #_(expect-> out (assert (= ["lww_crdt"
                              [[[4 100 "X"] 2 -1]
                               [[10 100 "Z"] 2 1]
                               [[10 200 "Z"] 2 1]]] out))))

  (exec! conn
    (transact db [{:db/id 3 :assign/time 6 :assign/key 200 :assign/value "Y"}])
    (transact db [{:db/id 3 :assign/time 1 :assign/key 200 :assign/value "Y"}]))

  )
