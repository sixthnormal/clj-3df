(ns clj-3df.confusion
  (:require
   [clj-3df.core :refer [create-db create-input
                         debug-conn
                         register-source register-query
                         exec! transact]]))

(def schema
  {:target  {:db/valueType :String}
   :guess   {:db/valueType :String}
   :country {:db/valueType :String}

   ;; wip
   :q1-country {:db/valueType :String}})

(def db (create-db schema))

(comment

  (def conn (debug-conn "ws://127.0.0.1:6262"))

  (exec! conn
    (create-input :q1-country)
    (register-source
     [:target :guess :country]
     {:JsonFile {:path "./data/confusion/xaa"}}))
  
  (exec! conn
    (register-query db "q1" '[:find (count ?guess)
                              :where
                              [?guess :target "Russian"]
                              [?guess :guess "Russian"]]))

  (exec! conn
    (register-query db "q1-parameterized" '[:find (count ?guess)
                                            :where
                                            [?param :q1-country ?country]
                                            [?guess :target ?country]
                                            [?guess :guess ?country]]))

  (exec! conn
    (transact db [[:db/retract 100000000 :q1-country "Russian"]
                  [:db/add 100000000 :q1-country "Latvian"]
                  [:db/add 100000001 :q1-country "German"]]))

  (exec! conn
    (register-query db "q2" '[:find ?country ?target (count ?guess)
                              :where
                              [?guess :target ?target]
                              [?guess :country ?country]]))

  (exec! conn
    (register-query db "q2-parameterized" '[:find ?country ?target (count ?guess)
                                            :where
                                            [?param :q1-country ?country]
                                            [?guess :target ?target]
                                            [?guess :country ?country]]))

  (exec! conn
    (transact db [{:db/id 99999999 :target "Russian" :guess "Russian" :country "RU"}]))

  )
