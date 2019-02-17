(ns clj-3df.ldbc
  (:require
   [clj-3df.core :as df :refer [exec!]]
   [clj-3df.binding :as binding]))

(def schema
  {:system/time {:db/valueType :Number}

   :person/id           {:db/valueType :Number}
   :person/firstname    {:db/valueType :String}
   :person/lastname     {:db/valueType :String}
   :person/gender       {:db/valueType :String}
   :person/birthday     {:db/valueType :String}
   :person/created-at   {:db/valueType :Number}
   :person/location-ip  {:db/valueType :String}
   :person/browser-used {:db/valueType :String}
   :person/likes-post   {:db/valueType :Eid}

   :post/creation-date {:db/valueType :String}

   :comment/creation-date  {:db/valueType :String}
   :comment/ip             {:db/valueType :String}
   :comment/browser        {:db/valueType :String}
   :comment/content        {:db/valueType :String}
   :comment/creator        {:db/valueType :Eid}
   :comment/place          {:db/valueType :Eid}
   :comment/parent-post    {:db/valueType :Eid}
   :comment/parent-comment {:db/valueType :Eid}
   })

(def db (df/create-db schema))

(def ldbc-defaults
  {:has_headers true
   :delimiter   (byte \|)
   :comment     nil
   :flexible    false})

(comment

  (do
    (def conn (df/create-debug-conn "ws://127.0.0.1:6262"))

    (exec! conn
      (df/register-sink
       "ldbc.sinks/csv"
       {:CsvFile {:has_headers false
                  :delimiter   (byte \|)
                  :flexible    false
                  :path        "/Users/niko/data/joined/sink.csv"}}))

    (exec! conn
      (df/register-source
       [#_:eid :person/firstname #_:lastname #_:gender #_:birthday #_:timestamp]
       {:CsvFile (merge ldbc-defaults
                        {:path             "/Users/niko/data/1k-users/person.csv"
                         :eid_offset       0
                         :timestamp_offset 5
                         :schema           [[1 {:String ""}]]})}))
    
    (exec! conn
      (df/register-source
       [#_:eid :post/creation-date #_:timestamp]
       {:CsvFile (merge ldbc-defaults
                        {:path             "/Users/niko/data/1k-users/post.csv"
                         :eid_offset       0
                         :timestamp_offset 2
                         :schema           [[2 {:String ""}]]})}))

    (exec! conn
      (df/register-source
       [#_:eid :comment/creation-date :comment/ip :comment/browser :comment/content]
       {:CsvFile (merge ldbc-defaults
                        {:path       "/Users/niko/data/1k-users/comment.csv"
                         :eid_offset 0
                         ;; :timestamp_offset 1
                         :schema     [[1 {:String ""}]
                                      [2 {:String ""}]
                                      [3 {:String ""}]
                                      [4 {:String ""}]]})}))

    (exec! conn
      (df/register-source
       [#_:eid :comment/creator]
       {:CsvFile (merge ldbc-defaults
                        {:path       "/Users/niko/data/1k-users/comment_hasCreator_person.csv"
                         :eid_offset 0
                         :schema     [[1 {:Eid 0}]]})})
      (df/register-source
       [#_:eid :comment/place]
       {:CsvFile (merge ldbc-defaults
                        {:path       "/Users/niko/data/1k-users/comment_isLocatedIn_place.csv"
                         :eid_offset 0
                         :schema     [[1 {:Eid 0}]]})})
      (df/register-source
       [#_:eid :comment/parent-comment]
       {:CsvFile (merge ldbc-defaults
                        {:path       "/Users/niko/data/1k-users/comment_replyOf_comment.csv"
                         :eid_offset 0
                         :schema     [[1 {:Eid 0}]]})})
      (df/register-source
       [#_:eid :comment/parent-post]
       {:CsvFile (merge ldbc-defaults
                        {:path       "/Users/niko/data/1k-users/comment_replyOf_post.csv"
                         :eid_offset 0
                         :schema     [[1 {:Eid 0}]]})}))

    #_(exec! conn
      (df/register-source
       [#_:eid :person/likes-post #_:timestamp]
       {:CsvFile (merge ldbc-defaults
                        {:path             "/Users/niko/data/1k-users/person_likes_post.csv"
                         :eid_offset       0
                         :timestamp_offset 2
                         :schema           [[1 {:Eid 0}]]})}))
    )

  (exec! conn
    (df/create-attribute :system/time :db.semantics/raw)
    (df/transact db [[:db/add 1 :system/time 0]]))

  ;; advance to now
  (exec! conn
    (df/advance-domain 1650360833))

  #_(exec! conn
    (df/query
     db "person-count"
     '[:find (count ?person)
       :where
       (or-join [?person]
         [?person :system/time ?any]
         [?person :person/firstname "Chengdong"])]))

  (exec! conn
    (df/query
     db "person-count"
     '[:find (count ?person)
       :where [?person :person/firstname "Chengdong"]]))

  #_(exec! conn
    (df/query
     db "likes"
     '[:find ?person (count ?post)
       :where
       [?person :person/likes-post ?post]
       [?post :post/creation-date ?creation-date]]))

  (exec! conn
    (df/register
     db "comment_event_stream"
     {:Hector
      {:variables '[?comment ?person ?creationDate ?ip ?browser
                    ?content ?parent-post ?parent-comment ?place]
       :bindings  [(binding/attribute '[?comment :comment/creation-date ?creationDate])
                   (binding/attribute '[?comment :comment/creator ?person])
                   (binding/attribute '[?comment :comment/ip ?ip])
                   (binding/attribute '[?comment :comment/browser ?browser])
                   (binding/attribute '[?comment :comment/content ?content])
                   (binding/attribute '[?comment :comment/place ?place])
                   (binding/optional-attribute '[?comment :comment/parent-post ?parent-post ""])
                   (binding/optional-attribute '[?comment :comment/parent-comment ?parent-comment ""])]}}
     [])
    (df/flow "comment_event_stream" "ldbc.sinks/csv"))
  
  )
