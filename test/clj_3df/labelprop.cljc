(ns clj-3df.labelprop
  (:require
   [clj-3df.core :as df :refer [exec!]]))

(def schema
  {:label {:db/valueType :Number}
   :edge  {:db/valueType :Eid}})

(def rules
  '[[(label ?x ?y) [?x :label ?y]]
    [(label ?x ?y) [?z :edge ?y] (label ?x ?z)]])

(def db (df/create-db schema))

(def q
  '[:find (count ?x ?y) :where (label ?x ?y)])

(comment

  (def conn (df/debug-conn "ws://127.0.0.1:6262"))

  (exec! conn
    (df/register-source
     [:edge]
     {:CsvFile {:path      "/Users/niko/data/labelprop/edges.httpd_df"
                :separator " "
                :schema    [[1 {:Eid 0}]]}})
    (df/register-source
     [:label]
     {:CsvFile {:path      "/Users/niko/data/labelprop/nodes.httpd_df"
                :separator " "
                :schema    [[1 {:Eid 0}]]}}))

  (exec! conn
    (df/query db "test" '[:find ?x ?y :where [?x :label ?y]]))
  
  (exec! conn
    (df/query db "labelprop" q rules))

  )
