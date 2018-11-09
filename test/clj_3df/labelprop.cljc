(ns clj-3df.labelprop
  (:require
   [clj-3df.core :as df]))

(def schema
  {:label {:db/valueType :Number}
   :edge  {:db/valueType :Eid}})

(def rules
  '[[(label ?x ?y) [?x :label ?y]]
    [(label ?x ?y) [?z :edge ?y] (label ?x ?z)]])

(def db (create-db schema))

(def q
  '[:find (count ?x ?y) :where (label ?x ?y)])

(comment

  (def conn (debug-conn "ws://127.0.0.1:6262"))

  (exec! conn
    (register-source ["edge"] {:PlainFile {:path "./data/labelprop/edges.httpd_df"}})
    (register-source ["label"] {:PlainFile {:path "./data/labelprop/nodes.httpd_df"}})
    )
  
  (exec! conn
    (register-query db "labelprop" q rules))

  )
