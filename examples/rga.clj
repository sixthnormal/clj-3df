(ns clj-3df.examples.rga
  (:require
   [clj-3df.core :refer [create-conn create-db exec!
                         register-plan register-query transact]]))

;; RGA
;; https://speakerdeck.com/ept/data-structures-as-queries-expressing-crdts-using-datalog?slide=22

(def schema
  {:id/node       {:db/valueType :Number}
   :id/ctr        {:db/valueType :Number}
   :insert/id     {:db/valueType :Eid}
   :insert/parent {:db/valueType :Eid}
   :assign/id     {:db/valueType :Eid}
   :assign/elem   {:db/valueType :Eid}
   :assign/value  {:db/valueType :String}
   :remove/id     {:db/valueType :Eid}})

(def db (create-db schema))

(def rules
  '[[(has-child? ?parent) [?wc :insert/parent ?parent]]

    [(later-child ?parent ?child2)
     [?i1 :insert/parent ?parent] [?i1 :insert/id ?child1]
     [?i2 :insert/parent ?parent] [?i2 :insert/id ?child2]
     (or
      (and
       [?child1 :id/ctr ?ctr1] [?child2 :id/ctr ?ctr2]
       [(> ?ctr1 ?ctr2)])
      (and
       [?child1 :id/ctr ?ctr] [?child1 :id/node ?n1]
       [?child2 :id/ctr ?ctr] [?child2 :id/node ?n2]
       [(> ?n1 ?n2)]))]

    [(first-child ?parent ?child)
     [?i :insert/id ?child] [?i :insert/parent ?parent]
     (not (later-child ?parent ?child))]

    [(sibling? ?child1 ?child2)
     [?i1 :insert/id ?child1] [?i1 :insert/parent ?parent]
     [?i2 :insert/id ?child2] [?i2 :insert/parent ?parent]]

    [(later-sibling ?sib1 ?sib2)
     (sibling? ?sib1 ?sib2)
     (later? ?sib1 ?sib2)]

    [(later-sibling2 ?sib1 ?sib3)
     (sibling? ?sib1 ?sib2)
     (sibling? ?sib1 ?sib3)
     (later? ?sib1 ?sib2)
     (later? ?sib2 ?sib3)]

    [(next-sibling ?sib1 ?sib2)
     (later-sibling ?sib1 ?sib2)
     (not (later-sibling ?sib1 ?sib2))]

    [(has-next-sibling? ?sib1) (later-sibling ?sib1 ?wc)]

    [(next-sibling-anc ?start ?next) (next-sibling ?start ?next)]
    [(next-sibling-anc ?start ?next)
     [?i1 :insert/id ?start] [?i1 :insert/parent ?parent]
     (next-sibling-anc ?parent ?next)
     (not (has-next-sibling? ?start))]

    [(next-elem ?prev ?next) (first-child ?prev ?next)]
    [(next-elem ?prev ?next)
     (next-sibling-anc ?prev ?next)
     (not (has-child? ?prev))]

    ;; Assigning values to list elements.

    [(current-value ?elem ?value)
     [?op :assign/id ?id] [?op :assign/elem ?elem] [?op :assign/value ?value]
     (not [?wc :remove/id ?id])]

    [(has-value? ?elem) (current-value ?elem ?wc)]

    [(skip-blank ?from ?to) (next-elem ?from ?to)]
    [(skip-blank ?from ?to)
     (next-elem ?from ?via)
     (not (has-value? ?via))
     (skip-blank ?via ?to)]

    [(next-visible ?prev ?next)
     (has-value? ?prev)
     (skip-blank ?prev ?next)
     (has-value? ?next)]

    ;; Output

    [(result ?ctr1 ?ctr2 ?value)
     [?id1 :id/ctr ?ctr1]
     [?id2 :id/ctr ?ctr2]
     (next-visible ?id1 ?id2)
     (current-value ?id2 ?value)]
    ])

;; query

(def q '[:find ?ctr1 ?ctr2 ?value :where (result ?ctr1 ?ctr2 ?value)])

;; test

(comment

  (def conn (create-conn "ws://127.0.0.1:6262"))

  (exec! conn (register-query db "rga" q rules))

  (exec! conn
    (transact db [{:db/id 1 :assign/time 4 :assign/key 100 :assign/value "X"}
                  {:db/id 2 :assign/time 2 :assign/key 100 :assign/value "Y"}])

    (transact db [{:db/id 4 :assign/time 10 :assign/key 100 :assign/value "Z"}
                  {:db/id 5 :assign/time 10 :assign/key 200 :assign/value "Z"}])

    (transact db [{:db/id 3 :assign/time 6 :assign/key 200 :assign/value "Y"}]))
  )
