(ns clj-3df.ldbc
  (:require
   [clj-3df.core :as df :refer [exec!]]
   [clj-3df.attribute :as attribute]
   [clj-3df.time :as time]
   [clj-3df.binding :as binding]))

(def schema
  {:timely/scope {:db/valueType :Eid}

   :timely.event.operates/local-id     {:db/valueType :Eid}
   :timely.event.operates/name         {:db/valueType :String}
   :timely.event.operates/address      {:db/valueType :Address}
   :timely.event.operates/shutdown?    {:db/valueType :Bool}
   :timely.event.channels/src-index    {:db/valueType :Eid}
   :timely.event.channels/src-port     {:db/valueType :Eid}
   :timely.event.channels/target-index {:db/valueType :Eid}
   :timely.event.channels/target-port  {:db/valueType :Eid}

   :differential.event/size {:db/valueType :Number}
   
   :comment/creation-date  {:db/valueType :String}
   :comment/ip             {:db/valueType :String}
   :comment/browser        {:db/valueType :String}
   :comment/content        {:db/valueType :String}
   :comment/creator        {:db/valueType :Eid}
   :comment/place          {:db/valueType :Eid}
   :comment/tag            {:db/valueType :Eid}
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

   :input.selector/person (merge
                           (attribute/of-type :Eid)
                           (attribute/input-semantics :db.semantics.cardinality/many)
                           (attribute/real-time))
   :input.selector/name   (merge
                           (attribute/of-type :String)
                           (attribute/input-semantics :db.semantics.cardinality/many)
                           (attribute/real-time))})

(defn timely? [kw]
  (clojure.string/starts-with? (namespace kw) "timely"))

(defn differential? [kw]
  (clojure.string/starts-with? (namespace kw) "differential"))

(def db (df/create-db schema))

(def ldbc-defaults
  {:has_headers true
   :delimiter   (byte \|)
   :comment     nil
   :flexible    false})

(def dataset-path "/opt/data/social_network/")

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
                            [attr [idx typ]]
                            (throw (ex-info "Unknown attribute." {:attribute attr}))))))
         (remove nil?))))

(do
  (assert
   (=
    (source-schema [nil :comment/creation-date :comment/ip :comment/browser :comment/content])
    [[:comment/creation-date [1 {:String ""}]]
     [:comment/ip [2 {:String ""}]]
     [:comment/browser [3 {:String ""}]]
     [:comment/content [4 {:String ""}]]]))
  (assert
   (=
    (source-schema [:membership/forum :membership/person :membership/join-date])
    [[:membership/forum [0 {:Eid 0}]]
     [:membership/person [1 {:Eid 0}]]
     [:membership/join-date [2 {:String ""}]]])))

(defn attribute-source
  "A source providing a single attribute (a stream of (e v)-pairs),
  complete with domain-specific timestamps and globally valid
  eids. This is the kind of source we like to work with."
  [attr file]
  (df/register-source
   {:CsvFile (merge ldbc-defaults
                    {:path       (str dataset-path file)
                     :eid_offset 0
                     ;; :timestamp_offset 2
                     :schema     (source-schema [nil attr])})}))

(defn untimed-source
  "A source providing a single attribute (a stream of (e v)-pairs),
  but without domain-specific timestamps. Eids are assumed to be
  globally valid."
  [attr file]
  (df/register-source
   {:CsvFile (merge ldbc-defaults
                    {:path       (str dataset-path file)
                     :eid_offset 0
                     :schema     (source-schema [nil attr])})}))

(defn advance-to-now!
  [conn]
  (let [now (.getEpochSecond (java.time.Instant/now))]
    (exec! conn
      (df/advance-domain (time/instant now)))))

(defn load-dataset!
  [conn]
  (exec! conn
    (let [attributes [nil :comment/creation-date :comment/ip :comment/browser :comment/content]]
      (df/register-source {:CsvFile (merge ldbc-defaults
                                           {:path       (str dataset-path "comment_0_0.csv")
                                            :eid_offset 0
                                            :schema     (source-schema attributes)})})))

  (exec! conn
    (let [attributes [nil :forum/title :forum/creation-date]]
      (df/register-source {:CsvFile (merge ldbc-defaults
                                           {:path       (str dataset-path "forum_0_0.csv")
                                            :eid_offset 0
                                            :schema     (source-schema attributes)})})))

  (exec! conn
    (let [attributes [nil :organisation/type :organisation/name :organisation/url]]
      (df/register-source {:CsvFile (merge ldbc-defaults
                                           {:path       (str dataset-path "organisation_0_0.csv")
                                            :eid_offset 0
                                            :schema     (source-schema attributes)})})))

  (exec! conn
    (let [attributes [nil :person/firstname :person/lastname :person/gender :person/birthday :person/creation-date :person/ip :person/browser]]
      (df/register-source {:CsvFile (merge ldbc-defaults
                                           {:path       (str dataset-path "person_0_0.csv")
                                            :eid_offset 0
                                            :schema     (source-schema attributes)})})))

  (exec! conn
    (let [attributes [nil :place/name :place/url :place/type]]
      (df/register-source {:CsvFile (merge ldbc-defaults
                                           {:path       (str dataset-path "place_0_0.csv")
                                            :eid_offset 0
                                            :schema     (source-schema attributes)})})))
  
  (exec! conn
    (let [attributes [nil :post/image-file :post/creation-date :post/ip :post/browser :post/language :post/content]]
      (df/register-source {:CsvFile (merge ldbc-defaults
                                           {:path       (str dataset-path "post_0_0.csv")
                                            :eid_offset 0
                                            :schema     (source-schema attributes)})})))
  
  (exec! conn
    (let [attributes [nil :tag/name :tag/url]]
      (df/register-source {:CsvFile (merge ldbc-defaults
                                           {:path       (str dataset-path "tag_0_0.csv")
                                            :eid_offset 0
                                            :schema     (source-schema attributes)})})))

  (exec! conn
    (let [attributes [nil :tagclass/name :tagclass/url]]
      (df/register-source {:CsvFile (merge ldbc-defaults
                                           {:path       (str dataset-path "tagclass_0_0.csv")
                                            :eid_offset 0
                                            :schema     (source-schema attributes)})})))
  
  (exec! conn
    (untimed-source :comment/creator "comment_hasCreator_person_0_0.csv")
    (untimed-source :comment/tag "comment_hasTag_tag_0_0.csv")
    (untimed-source :comment/place "comment_isLocatedIn_place_0_0.csv")
    (untimed-source :comment/parent-comment "comment_replyOf_comment_0_0.csv")
    (untimed-source :comment/parent-post "comment_replyOf_post_0_0.csv"))

  (exec! conn
    (untimed-source :forum/post "forum_containerOf_post_0_0.csv")
    (untimed-source :forum/tag "forum_hasTag_tag_0_0.csv")
    (untimed-source :forum/moderator "forum_hasModerator_person_0_0.csv"))

  (exec! conn
    (let [attributes [:membership/forum :membership/person :membership/join-date]]
      (df/register-source {:CsvFile (merge ldbc-defaults
                                           {:path       (str dataset-path "forum_hasMember_person_0_0.csv")
                                            :eid_offset 0
                                            :schema     (source-schema attributes)})})))

  (exec! conn
    (untimed-source :organisation/place "organisation_isLocatedIn_place_0_0.csv"))

  (exec! conn
    (untimed-source :person/email "person_email_emailaddress_0_0.csv")
    (untimed-source :person/interest "person_hasInterest_tag_0_0.csv")
    (untimed-source :person/place "person_isLocatedIn_place_0_0.csv")
    (untimed-source :person/knows-person "person_knows_person_0_0.csv")
    (untimed-source :person/likes-post "person_likes_post_0_0.csv")
    (untimed-source :person/speaks "person_speaks_language_0_0.csv"))

  (exec! conn
    (let [attributes [:study-at/person :study-at/organisation :study-at/class-year]]
      (df/register-source {:CsvFile (merge ldbc-defaults
                                           {:path       (str dataset-path "person_studyAt_organisation_0_0.csv")
                                            :eid_offset 0
                                            :schema     (source-schema attributes)})})))
  
  (exec! conn
    (let [attributes [:work-at/person :work-at/organisation :work-at/work-from]]
      (df/register-source {:CsvFile (merge ldbc-defaults
                                           {:path       (str dataset-path "person_workAt_organisation_0_0.csv")
                                            :eid_offset 0
                                            :schema     (source-schema attributes)})})))

  (exec! conn
    (untimed-source :place/part-of "place_isPartOf_place_0_0.csv"))

  (exec! conn
    (untimed-source :post/creator "post_hasCreator_person_0_0.csv")
    (untimed-source :post/tag "post_hasTag_tag_0_0.csv")
    (untimed-source :post/place "post_isLocatedIn_place_0_0.csv"))

  (exec! conn
    (untimed-source :tag/class "tag_hasType_tagclass_0_0.csv"))

  (exec! conn
    (untimed-source :tagclass/superclass "tagclass_isSubclassOf_tagclass_0_0.csv")))

(comment

  (def conn (df/create-debug-conn! "ws://127.0.0.1:6262"))

  (exec! conn
    (df/create-attribute :input.selector/name (get schema :input.selector/name)))

  (exec! conn
    (let [name (name (gensym "test"))]
      (concat
       (df/register-query
        db name
        '[:find ?name
          :where
          [?comment :comment/browser ?name]
          [?in :input.selector/name ?name]])
       [{:Interest {:name        name
                    :granularity (time/real-time 1)}}])))

  (exec! conn
    (df/transact db [[:db/add 0 :input.selector/name "Firefox"]]))
  
  
  )
