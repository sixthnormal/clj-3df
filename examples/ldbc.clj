(ns clj-3df.ldbc
  (:require
   [clj-3df.core :as df :refer [exec!]]))

(def schema
  {:person/id           {:db/valueType :Number}
   :person/firstname    {:db/valueType :String}
   :person/lastname     {:db/valueType :String}
   :person/gender       {:db/valueType :String}
   :person/birthday     {:db/valueType :String}
   :person/created-at   {:db/valueType :Number}
   :person/location-ip  {:db/valueType :String}
   :person/browser-used {:db/valueType :String}})

(def db (df/create-db schema))

(comment

  (def conn (df/debug-conn "ws://127.0.0.1:6262"))

  (exec! conn
    (df/register-source
     [#_:person/id :person/firstname :person/lastname :person/gender :person/birthday #_:person/created-at :person/location-ip :person/browser-used]
     {:CsvFile {:path      "/Users/niko/data/social_network/person_0_0.csv"
                :separator "|"
                :schema    [#_[0 {:Number 0}]
                            [1 {:String ""}]
                            [2 {:String ""}]
                            [3 {:String ""}]
                            [4 {:String ""}]
                            ;; [5 {:Number 0}]
                            [6 {:String ""}]
                            [7 {:String ""}]]}}))

  (exec! conn
    (df/register-query
     db "test"
     '[:find ?p1 
       :where
       [?p1 :person/firstname "firstName"]]))
  
  )
