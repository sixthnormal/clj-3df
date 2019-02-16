(ns clj-3df.ldbc
  (:require
   [clj-3df.core :as df :refer [exec!]]))

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

   :comment/ip      {:db/valueType :String}
   :comment/browser {:db/valueType :String}
   :comment/content {:db/valueType :String}
   :comment/creator {:db/valueType :Eid}
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
       [#_:eid #_timestamp :comment/ip :comment/browser :comment/content]
       {:CsvFile (merge ldbc-defaults
                        {:path             "/Users/niko/data/1k-users/comment.csv"
                         :eid_offset       0
                         :timestamp_offset 1
                         :schema           [[2 {:String ""}]
                                            [3 {:String ""}]
                                            [4 {:String ""}]]})}))

    (exec! conn
      (df/register-source
       [#_:eid :comment/creator]
       {:CsvFile (merge ldbc-defaults
                        {:path       "/Users/niko/data/1k-users/comment_hasCreator_person.csv"
                         :eid_offset 0
                         :schema     [[1 {:Eid 0}]]})}))

    (exec! conn
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
    (df/advance-domain 1550325946))

  (exec! conn
    (df/query
     db "person-count"
     '[:find (count ?person)
       :where
       (or-join [?person]
         [?person :system/time ?any]
         [?person :person/firstname "Chengdong"])]))

  (exec! conn
    (df/query
     db "likes"
     '[:find ?person (count ?post)
       :where
       [?person :person/likes-post ?post]
       [?post :post/creation-date ?creation-date]]))

  (exec! conn
    (df/register-query
     db "comment_event_stream"
     '[:find
       #_?id ?person #_?creationDate #_?ip #_?browser
       #_?content #_?reply_to_postId #_?reply_to_commentId #_?placeId
       :where
       [?person :person/firstname "Chengdong"]
       ;; [?comment :comment/creator ?person]
       ;; [?comment :comment/ip ?ip]
       ;; [?comment :comment/browser ?browser]
       #_[?comment :comment/content ?content]])
    (df/flow "comment_event_stream" "ldbc.sinks/csv"))
  
  )
