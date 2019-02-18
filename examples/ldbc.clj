(ns clj-3df.ldbc
  (:require
   [clj-3df.core :as df :refer [exec!]]
   [clj-3df.binding :as binding]))

(def schema
  {:comment/creation-date  {:db/valueType :String}
   :comment/ip             {:db/valueType :String}
   :comment/browser        {:db/valueType :String}
   :comment/content        {:db/valueType :String}
   :comment/creator        {:db/valueType :Eid}
   :comment/place          {:db/valueType :Eid}
   :comment/parent-post    {:db/valueType :Eid}
   :comment/parent-comment {:db/valueType :Eid}

   :forum/title         {:db/valueType :String}
   :forum/creation-date {:db/valueType :String}
   :forum/post          {:db/valueType :Eid}
   :forum/tag           {:db/valueType :Eid}
   :forum/moderator     {:db/valueType :Eid}

   :membership/forum     {:db/valueType :Eid}
   :membership/person    {:db/valueType :Eid}
   :membership/join-date {:db/valueType :String}

   :organisation/type  {:db/valueType :String}
   :organisation/name  {:db/valueType :String}
   :organisation/url   {:db/valueType :String}
   :organisation/place {:db/valueType :Eid}
   
   ;; :person/id            {:db/valueType :Number}
   :person/firstname     {:db/valueType :String}
   :person/lastname      {:db/valueType :String}
   :person/gender        {:db/valueType :String}
   :person/birthday      {:db/valueType :String}
   :person/creation-date {:db/valueType :String}
   :person/ip            {:db/valueType :String}
   :person/browser       {:db/valueType :String}
   :person/email         {:db/valueType :String}
   :person/interest      {:db/valueType :Eid}
   :person/place         {:db/valueType :Eid}
   :person/knows-person  {:db/valueType :Eid}
   :person/likes-post    {:db/valueType :Eid}
   :person/speaks        {:db/valueType :String}

   :study-at/person       {:db/valueType :Eid}
   :study-at/organisation {:db/valueType :Eid}
   :study-at/class-year   {:db/valueType :Number}

   :work-at/person       {:db/valueType :Eid}
   :work-at/organisation {:db/valueType :Eid}
   :work-at/work-from    {:db/valueType :Number}

   :place/name    {:db/valueType :String}
   :place/url     {:db/valueType :String}
   :place/type    {:db/valueType :String}
   :place/part-of {:db/valueType :Eid}

   :post/image-file    {:db/valueType :String}
   :post/creation-date {:db/valueType :String}
   :post/ip            {:db/valueType :String}
   :post/browser       {:db/valueType :String}
   :post/language      {:db/valueType :String}
   :post/content       {:db/valueType :String}
   :post/creator       {:db/valueType :Eid}
   :post/tag           {:db/valueType :Eid}
   :post/place         {:db/valueType :Eid}

   :tag/name  {:db/valueType :String}
   :tag/url   {:db/valueType :String}
   :tag/class {:db/valueType :Eid}

   :tagclass/name       {:db/valueType :String}
   :tagclass/url        {:db/valueType :String}
   :tagclass/superclass {:db/valueType :Eid}
   })

(def db (df/create-db schema))

(def ldbc-defaults
  {:has_headers true
   :delimiter   (byte \|)
   :comment     nil
   :flexible    false})

(def dataset-path "/Users/niko/data/1k-users/")

(def encode-type
  {:String {:String ""}
   :Eid    {:Eid 0}
   :Number {:Number 0}})

(defn source-schema
  [attributes]
  (let [schema->type (fn [attr]
                       (-> schema (get-in [attr :db/valueType]) encode-type))]
    (->> attributes
         (map-indexed (fn [idx attr]
                        (when (some? attr)
                          (if-let [typ (schema->type attr)]
                            [idx typ]
                            (throw (ex-info "Unknown attribute." {:attribute attr}))))))
         (remove nil?))))

(do
  (assert
   (=
    (source-schema [nil :comment/creation-date :comment/ip :comment/browser :comment/content])
    [[1 {:String ""}]
     [2 {:String ""}]
     [3 {:String ""}]
     [4 {:String ""}]]))
  (assert
   (=
    (source-schema [:membership/forum :membership/person :membership/join-date])
    [[0 {:Eid 0}]
     [1 {:Eid 0}]
     [2 {:String ""}]])))

;; (defn table-source
;;   [attributes file]
;;   (df/register-source
;;    attributes
;;    {:CsvFile (merge ldbc-defaults
;;                     {:path             (str dataset-path file)
;;                      :eid_offset       0
;;                      :timestamp_offset 1
;;                      :schema           (source-schema attributes)})}))

(defn attribute-source
  "A source providing a single attribute (a stream of (e v)-pairs),
  complete with domain-specific timestamps and globally valid
  eids. This is the kind of source we like to work with."
  [attr file]
  (df/register-source
   [#_:eid attr #_:timestamp]
   {:CsvFile (merge ldbc-defaults
                    {:path             (str dataset-path file)
                     :eid_offset       0
                     :timestamp_offset 2
                     :schema           (source-schema [nil attr])})}))

(defn untimed-source
  "A source providing a single attribute (a stream of (e v)-pairs),
  but without domain-specific timestamps. Eids are assumed to be
  globally valid."
  [attr file]
  (df/register-source
   [#_:eid attr]
   {:CsvFile (merge ldbc-defaults
                    {:path       (str dataset-path file)
                     :eid_offset 0
                     :schema     (source-schema [nil attr])})}))

(defn hapless-source
  "A source providing a single attribute (a stream of (e v)-pairs),
  but with neither domain-specific timestamps, not globally valid
  eids."
  [attr file]
  (df/register-source
   [#_:eid attr]
   {:CsvFile (merge ldbc-defaults
                    {:path   (str dataset-path file)
                     :schema [[1 {:Eid 0}]]})}))

(comment
  (do
    (def conn (df/create-debug-conn "ws://127.0.0.1:6262"))

    (exec! conn
      (let [attributes [nil :comment/creation-date :comment/ip :comment/browser :comment/content]]
        (df/register-source (remove nil? attributes)
                            {:CsvFile (merge ldbc-defaults
                                             {:path             (str dataset-path "comment.csv")
                                              :eid_offset       0
                                              :timestamp_offset 1
                                              :schema           (source-schema attributes)})})))

    (exec! conn
      (let [attributes [nil :forum/title :forum/creation-date]]
        (df/register-source (remove nil? attributes)
                            {:CsvFile (merge ldbc-defaults
                                             {:path             (str dataset-path "forum.csv")
                                              :eid_offset       0
                                              :timestamp_offset 2
                                              :schema           (source-schema attributes)})})))

    (exec! conn
      (let [attributes [nil :organisation/type :organisation/name :organisation/url]]
        (df/register-source (remove nil? attributes)
                            {:CsvFile (merge ldbc-defaults
                                             {:path       (str dataset-path "organisation.csv")
                                              :eid_offset 0
                                              :schema     (source-schema attributes)})})))

    (exec! conn
      (let [attributes [nil :person/firstname :person/lastname :person/gender :person/birthday :person/creation-date :person/ip :person/browser]]
        (df/register-source (remove nil? attributes)
                            {:CsvFile (merge ldbc-defaults
                                             {:path             (str dataset-path "person.csv")
                                              :eid_offset       0
                                              :timestamp_offset 5
                                              :schema           (source-schema attributes)})})))

    (exec! conn
      (let [attributes [nil :place/name :place/url :place/type]]
        (df/register-source (remove nil? attributes)
                            {:CsvFile (merge ldbc-defaults
                                             {:path       (str dataset-path "place.csv")
                                              :eid_offset 0
                                              :schema     (source-schema attributes)})})))
    
    (exec! conn
      (let [attributes [nil :post/image-file :post/creation-date :post/ip :post/browser :post/language :post/content]]
        (df/register-source (remove nil? attributes)
                            {:CsvFile (merge ldbc-defaults
                                             {:path             (str dataset-path "post.csv")
                                              :eid_offset       0
                                              :timestamp_offset 2
                                              :schema           (source-schema attributes)})})))
    
    (exec! conn
      (let [attributes [nil :tag/name :tag/url]]
        (df/register-source (remove nil? attributes)
                            {:CsvFile (merge ldbc-defaults
                                             {:path       (str dataset-path "tag.csv")
                                              :eid_offset 0
                                              :schema     (source-schema attributes)})})))

    (exec! conn
      (let [attributes [nil :tagclass/name :tagclass/url]]
        (df/register-source (remove nil? attributes)
                            {:CsvFile (merge ldbc-defaults
                                             {:path       (str dataset-path "tagclass.csv")
                                              :eid_offset 0
                                              :schema     (source-schema attributes)})})))
    
    (exec! conn
      (untimed-source :comment/creator "comment_hasCreator_person.csv")
      (untimed-source :comment/place "comment_isLocatedIn_place.csv")
      (untimed-source :comment/parent-comment "comment_replyOf_comment.csv")
      (untimed-source :comment/parent-post "comment_replyOf_post.csv"))

    (exec! conn
      (untimed-source :forum/post "forum_containerOf_post.csv")
      (untimed-source :forum/tag "forum_hasTag_tag.csv")
      (untimed-source :forum/moderator "forum_hasModerator_person.csv"))

    (exec! conn
      (let [attributes [:membership/forum :membership/person :membership/join-date]]
        (df/register-source (remove nil? attributes)
                            {:CsvFile (merge ldbc-defaults
                                             {:path             (str dataset-path "forum_hasMember_person.csv")
                                              :eid_offset       0
                                              :timestamp_offset 2
                                              :schema           (source-schema attributes)})})))

    (exec! conn
      (untimed-source :organisation/place "organisation_isLocatedIn_place.csv"))

    (exec! conn
      (untimed-source :person/email "person_email_emailaddress.csv")
      (untimed-source :person/interest "person_hasInterest_tag.csv")
      (untimed-source :person/place "person_isLocatedIn_place.csv")
      (untimed-source :person/knows-person "person_knows_person.csv")
      (attribute-source :person/likes-post "person_likes_post.csv")
      (untimed-source :person/speaks "person_speaks_language.csv"))

    (exec! conn
      (let [attributes [:study-at/person :study-at/organisation :study-at/class-year]]
        (df/register-source (remove nil? attributes)
                            {:CsvFile (merge ldbc-defaults
                                             {:path       (str dataset-path "person_studyAt_organisation.csv")
                                              :eid_offset 0
                                              :schema     (source-schema attributes)})})))
    
    (exec! conn
      (let [attributes [:work-at/person :work-at/organisation :work-at/work-from]]
        (df/register-source (remove nil? attributes)
                            {:CsvFile (merge ldbc-defaults
                                             {:path       (str dataset-path "person_workAt_organisation.csv")
                                              :eid_offset 0
                                              :schema     (source-schema attributes)})})))

    (exec! conn
      (untimed-source :place/part-of "place_isPartOf_place.csv"))

    (exec! conn
      (untimed-source :post/creator "post_hasCreator_person.csv")
      (untimed-source :post/tag "post_hasTag_tag.csv")
      (untimed-source :post/place "post_isLocatedIn_place.csv"))

    (exec! conn
      (untimed-source :tag/class "tag_hasType_tagclass.csv"))

    (exec! conn
      (untimed-source :tagclass/superclass "tagclass_isSubclassOf_tagclass.csv"))

    ;; advance to now
    (let [now (.toEpochMilli (java.time.Instant/now))]
      (exec! conn
        (df/advance-domain now)))
    )
  )


(comment

  (exec! conn
    (df/register-sink
     "ldbc.sinks/csv"
     {:CsvFile {:has_headers false
                :delimiter   (byte \|)
                :flexible    false
                :path        "/Users/niko/data/joined/sink.csv"}})
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
