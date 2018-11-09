(ns clj-3df.capm
  (:require
   [clj-3df.core :refer [create-db create-input
                         debug-conn
                         register-source register-query
                         exec! transact]]))

(def schema
  {:user      {:db/valueType :Number}
   :timestamp {:db/valueType :Number}
   :link/uri  {:db/valueType :String}})

(def db (create-db schema))

(comment

  (def conn (debug-conn "ws://127.0.0.1:6262"))

  (exec! conn
    (register-source
     [:user :timestamp :link/uri]
     {:JsonFile {:path "./data/capm/analytics_dump.json"}}))
  
  (exec! conn
    (register-query db "links" '[:find ?uri (count ?e)
                                 :where [?e :link/uri ?uri]]))

  )
