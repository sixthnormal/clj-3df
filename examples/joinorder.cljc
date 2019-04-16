(ns clj-3df.joinorder
  (:require
   [clj-3df.core :as df :refer [exec!]]
   [clj-3df.attribute :as attribute]
   [clj-3df.time :as time]
   [clj-3df.binding :as binding]))

(def schema
  {:differential.event/size {:db/valueType :Number}
   
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

   :input.selector/person    (merge
                              (attribute/of-type :Eid)
                              (attribute/input-semantics :db.semantics.cardinality/many)
                              (attribute/real-time))
   :input.selector/firstname (merge
                              (attribute/of-type :String)
                              (attribute/input-semantics :db.semantics.cardinality/many)
                              (attribute/real-time))})

(def db (df/create-db schema))

(def ldbc-defaults
  {:has_headers true
   :delimiter   (byte \|)
   :comment     nil
   :flexible    false})

(def dataset-path "/Users/niko/data/social_network/")

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
    (df/register-source
     {:DifferentialLogging
      {:attributes [:differential.event/size]}})

    (df/register-query
     db "differential/tuples"
     '[:find (sum ?size)
       :where [?x :differential.event/size ?size]])

    [{:Interest {:name        "differential/tuples"
                 :granularity 1}}])

  (load-dataset! conn)

  ;; simple query to get things to settle in
  (exec! conn
    (df/query
     db (name (gensym "test"))
     '[:find (count ?person)
       :where
       [?x :place/part-of ?country]
       [?country :place/name "Germany"]
       [?person :person/place ?x]]))

  (exec! conn
    (concat
     (df/register-query
      db "bc2-reorder"
      '[:find ?msg ?creationDate ?tagname ?creator ?bday ?gender
        :where
        [?country :place/name "Germany"]
        [?place :place/part-of ?country]
        [?creator :person/place ?place]
        [?creator :person/birthday ?bday]
        [?creator :person/gender ?gender]
        [?msg :post/creator ?creator]
        [?msg :post/creation-date ?creationDate]
        [?msg :post/tag ?tag]
        [?tag :tag/name ?tagname]])
     [{:Interest {:name "bc2-reorder"
                  :sink {:TheVoid "/Users/niko/data/results/joinorder/times_reorder.csv"}}}]))

  (exec! conn
    (concat
     (df/register-query
      db "bc2-suboptimal"
      '[:find ?msg ?creationDate ?tagname ?creator ?bday ?gender
        :where
        [?place :place/part-of ?country]
        [?creator :person/birthday ?bday]
        [?creator :person/gender ?gender]
        [?tag :tag/name ?tagname]
        [?creator :person/place ?place]
        [?msg :post/creation-date ?creationDate]
        [?msg :post/tag ?tag]
        [?country :place/name "Germany"]
        [?msg :post/creator ?creator]])
     [{:Interest {:name "bc2-suboptimal"
                  :sink {:TheVoid "/Users/niko/data/results/joinorder/times_suboptimal.csv"}}}]))

  (exec! conn
    (let [name (name (gensym "bc2-hector-"))]
      (concat
       (df/register
        db name
        {:Hector
         {:variables '[?msg ?creationDate ?tagname ?creator ?bday ?gender]
          :bindings  [(binding/attribute '[?place :place/part-of ?country])
                      (binding/attribute '[?creator :person/birthday ?bday])
                      (binding/attribute '[?creator :person/gender ?gender])
                      (binding/attribute '[?tag :tag/name ?tagname])
                      (binding/attribute '[?creator :person/place ?place])
                      (binding/attribute '[?msg :post/creation-date ?creationDate])
                      (binding/attribute '[?msg :post/tag ?tag])
                      (binding/attribute '[?country :place/name ?country_name])
                      (binding/constant '[?country_name "Germany"])
                      (binding/attribute '[?msg :post/creator ?creator])]}}
        [])
       [{:Interest {:name name
                    :sink {:TheVoid "/Users/niko/data/results/joinorder/times_hector.csv"}}}])))

  (exec! conn
    (let [name (name (gensym "bi11-unrelated-replies"))]
      (concat
       (df/register-query
        db name
        '[:find ?comment ?tag ?post ?creator ?place ?country
          :where
          [?country :place/name "China"]
          [?place :place/part-of ?country]
          [?creator :person/place ?place]
          [?comment :comment/creator ?creator]
          [?comment :comment/parent-post ?post]
          [?post :post/tag ?tag]
          [?comment :comment/tag ?tag]])
       [{:Interest {:name name
                    :sink {:TheVoid (str "/Users/niko/data/results/joinorder/" name ".csv")}}}])))

  (exec! conn
    (let [name "bi11-unrelated-replies-hector5"]
      (concat
       (df/register
        db name
        {:Hector
         {:variables '[?comment ?tag ?post]
          :bindings  [(binding/attribute '[?comment :comment/parent-post ?post])
                      (binding/attribute '[?post :post/tag ?tag])
                      (binding/attribute '[?comment :comment/tag ?tag])
                      (binding/attribute '[?country :place/name ?arg_0])
                      (binding/constant '[?arg_0 "China"])
                      (binding/attribute '[?comment :comment/creator ?creator])
                      (binding/attribute '[?creator :person/place ?place])
                      (binding/attribute '[?place :place/part-of ?country])]}}
        [])
       [{:Interest {:name name
                    :sink {:TheVoid (str "/Users/niko/data/results/joinorder/" name ".csv")}}}])))

  (exec! conn
    (df/uninterest "bc2-reorder"))
  
  )


;; Let's investigate the number of tuples materialized throughout a
;; wco join.
(comment

  (exec! conn
    (let [name (name (gensym "materialized"))]
      (concat
       (df/register
        db name
        {:Hector
         {:variables '[?person ?firstname ?lastname ?gender ?bday]
          :bindings  [(binding/attribute '[?person :person/firstname ?firstname])
                      (binding/attribute '[?person :person/lastname ?lastname])
                      (binding/attribute '[?person :person/gender ?gender])
                      (binding/attribute '[?person :person/birthday ?bday])]}}
        [])
       [{:Interest {:name name
                    :sink {:TheVoid (str "/Users/niko/data/results/joinorder/" name ".csv")}}}])))

  )
