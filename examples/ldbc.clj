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

   :input.selector/person    (merge
                              (attribute/of-type :Eid)
                              (attribute/input-semantics :db.semantics.cardinality/many)
                              (attribute/real-time))
   :input.selector/firstname (merge
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

;; (def dataset-path "/Users/niko/data/social_network/")
  (def dataset-path "/opt/docker/volumes/ldbc/10k/")

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

#_(defn load-dataset!
  [conn]
  (exec! conn
    (let [attributes [nil :comment/creation-date :comment/ip :comment/browser :comment/content]]
      (df/register-source {:CsvFile (merge ldbc-defaults
                                           {:path       (str dataset-path "comment.csv")
                                            :eid_offset 0
                                            ;; :timestamp_offset 1
                                            :schema     (source-schema attributes)})})))

  (exec! conn
    (let [attributes [nil :forum/title :forum/creation-date]]
      (df/register-source {:CsvFile (merge ldbc-defaults
                                           {:path       (str dataset-path "forum.csv")
                                            :eid_offset 0
                                            ;; :timestamp_offset 2
                                            :schema     (source-schema attributes)})})))

  (exec! conn
    (let [attributes [nil :organisation/type :organisation/name :organisation/url]]
      (df/register-source {:CsvFile (merge ldbc-defaults
                                           {:path       (str dataset-path "organisation.csv")
                                            :eid_offset 0
                                            :schema     (source-schema attributes)})})))

  (exec! conn
    (let [attributes [nil :person/firstname :person/lastname :person/gender :person/birthday :person/creation-date :person/ip :person/browser]]
      (df/register-source {:CsvFile (merge ldbc-defaults
                                           {:path       (str dataset-path "person.csv")
                                            :eid_offset 0
                                            ;; :timestamp_offset 5
                                            :schema     (source-schema attributes)})})))

  (exec! conn
    (let [attributes [nil :place/name :place/url :place/type]]
      (df/register-source {:CsvFile (merge ldbc-defaults
                                           {:path       (str dataset-path "place.csv")
                                            :eid_offset 0
                                            :schema     (source-schema attributes)})})))
  
  (exec! conn
    (let [attributes [nil :post/image-file :post/creation-date :post/ip :post/browser :post/language :post/content]]
      (df/register-source {:CsvFile (merge ldbc-defaults
                                           {:path       (str dataset-path "post.csv")
                                            :eid_offset 0
                                            ;; :timestamp_offset 2
                                            :schema     (source-schema attributes)})})))
  
  (exec! conn
    (let [attributes [nil :tag/name :tag/url]]
      (df/register-source {:CsvFile (merge ldbc-defaults
                                           {:path       (str dataset-path "tag.csv")
                                            :eid_offset 0
                                            :schema     (source-schema attributes)})})))

  (exec! conn
    (let [attributes [nil :tagclass/name :tagclass/url]]
      (df/register-source {:CsvFile (merge ldbc-defaults
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
      (df/register-source {:CsvFile (merge ldbc-defaults
                                           {:path       (str dataset-path "forum_hasMember_person.csv")
                                            :eid_offset 0
                                            ;; :timestamp_offset 2
                                            :schema     (source-schema attributes)})})))

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
      (df/register-source {:CsvFile (merge ldbc-defaults
                                           {:path       (str dataset-path "person_studyAt_organisation.csv")
                                            :eid_offset 0
                                            :schema     (source-schema attributes)})})))
  
  (exec! conn
    (let [attributes [:work-at/person :work-at/organisation :work-at/work-from]]
      (df/register-source {:CsvFile (merge ldbc-defaults
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

  #_(advance-to-now! conn))



(comment

  ;; Setup.
  ;; 1. Create a connection and define a schema.
  ;; [2. Register Logging Streams]
  ;; 3. Load Dataset.
  ;; 4. Execute a simple query to advance traces.
  
  ;; (def conn (df/create-debug-conn! "ws://127.0.0.1:6262"))
  (def conn (df/create-debug-conn! "ws://caliban.c.clockworks.io:6262"))

  (exec! conn
    (concat
     (df/create-attribute :input.selector/person (get schema :input.selector/person))
     (df/create-attribute :input.selector/firstname (get schema :input.selector/firstname))

     (df/register-source
      {:TimelyLogging
       {:attributes (into [] (filter timely?) (keys schema))}})
     
     (df/register-source
      {:DifferentialLogging
       {:attributes [:differential.event/size]}})

     (df/register-query
      db "differential/tuples"
      '[:find #_?name (sum ?size)
        :where
        [?operator :differential.event/size ?size]
        #_[?operator :timely.event.operates/name ?name]])
     [{:Interest {:name            "differential/tuples"
                  :granularity     10
                  :disable_logging true}}]))

  (exec! conn
    (df/register-source
     {:DeclarativeLogging {:attributes []}}))

  (exec! conn
    (let [attributes [nil :person/firstname :person/lastname :person/gender :person/birthday :person/creation-date :person/ip :person/browser]]
      (df/register-source {:CsvFile (merge ldbc-defaults
                                           {:path       (str dataset-path "person_0_0.csv")
                                            :eid_offset 0
                                            :schema     (source-schema attributes)})})))
  
  (load-dataset! conn)

  (exec! conn
    (df/query
     db (name (gensym "purge"))
     '[:find (count ?x) :where [?x :person/browser ?y]]))

  (exec! conn
    (let [name (name (gensym "purge-hector"))]
      (println name)
      (concat
       (df/register
        db name
        {:Hector
         {:variables '[?gender #_?bday ?firstname ?browser]
          :bindings  [(binding/attribute '[?person :person/gender ?gender])
                      #_(binding/attribute '[?person :person/birthday ?bday])
                      (binding/attribute '[?person :person/firstname ?firstname])
                      (binding/attribute '[?person :person/browser ?browser])
                      (binding/constant '[?firstname "Anastasia"])]}}
        [])
       [{:Interest {:name name}}])))

  (exec! conn
    (df/uninterest "purge"))
  (* 9892 7)
  ;; => 69244
  )



(comment

  ;; Evaluation of an average-case query.
  ;; An example of this is BI/read/2, joins countries, cities,
  ;; persons, messages, and tags.

  (exec! conn
    (let [name (name (gensym "bi-read-2"))]
      (println name)
      (concat
       (df/register-query
        db name
        '[:find ?country ?bday ?gender ?person ?tag
          :where
          [?person :person/gender ?gender]
          [?person :person/birthday ?bday]
          [?person :person/place ?city]
          [?city :place/part-of ?country]
          [?country :place/name ?country-name]
          [?post :post/creator ?person]
          [?post :post/tag ?tag]
          [?tag :tag/name ?tag-name]])
       [{:Interest {:name name
                    :sink {:TheVoid "/home/niko/results/joinorder/bi2_join.csv"}
                    #_:sink #_{:TheVoid "/Users/niko/data/results/joinorder/bi_read_2_join.csv"}}}])))

  (exec! conn
    (let [name (name (gensym "bi-read-2-bad-order"))]
      (println name)
      (concat
       (df/register-query
        db name
        '[:find ?country ?bday ?gender ?person ?tag
          :where
          [?post :post/creator ?person]
          [?post :post/tag ?tag]
          [?tag :tag/name ?tag-name]
          [?city :place/part-of ?country]
          [?person :person/gender ?gender]
          [?person :person/birthday ?bday]
          [?person :person/place ?city]
          [?country :place/name ?country-name]])
       [{:Interest {:name name
                    :sink {:TheVoid "/home/niko/results/joinorder/bi2_join2.csv"}
                    #_:sink #_{:TheVoid "/Users/niko/data/results/joinorder/bi_read_2_join_bad_order.csv"}}}])))

  (exec! conn
    (let [name (name (gensym "bi-read-2-hector"))]
      (println name)
      (concat
       (df/register
        db name
        {:Hector
         {:variables '[?country ?bday ?gender ?person ?tag]
          :bindings  [(binding/attribute '[?post :post/creator ?person])
                      (binding/attribute '[?post :post/tag ?tag])
                      (binding/attribute '[?tag :tag/name ?tag-name])
                      (binding/attribute '[?city :place/part-of ?country])
                      (binding/attribute '[?person :person/gender ?gender])
                      (binding/attribute '[?person :person/birthday ?bday])
                      (binding/attribute '[?person :person/place ?city])
                      (binding/attribute '[?country :place/name ?country-name])]}}
        [])
       [{:Interest {:name name
                    :sink {:TheVoid "/home/niko/results/joinorder/bi2_hector.csv"}
                    #_:sink #_{:TheVoid "/Users/niko/data/results/joinorder/bi_read_2_hector.csv"}}}])))
  
  )



(comment

  ;; Evaluation of a best-case query.

  (exec! conn
    (let [name (name (gensym "best-case"))]
      (println name)
      (concat
       (df/register-query
        db name
        '[:find ?post ?ip ?browser ?language ?content
          :where
          [?post :post/ip ?ip]
          [?post :post/browser ?browser]
          [?post :post/language ?language]
          [?post :post/content ?content]])
       [{:Interest {:name name
                    :sink {:TheVoid "/home/niko/results/joinorder/best_case.csv"}
                    #_:sink #_{:TheVoid "/Users/niko/data/results/joinorder/best_case.csv"}}}])))

  (exec! conn
    (let [name (name (gensym "best-case-reorder"))]
      (println name)
      (concat
       (df/register-query
        db name
        '[:find ?post ?ip ?browser ?language ?content
          :where
          [?post :post/content ?content]
          [?post :post/language ?language]
          [?post :post/browser ?browser]
          [?post :post/ip ?ip]])
       [{:Interest {:name name
                    :sink {:TheVoid "/home/niko/results/joinorder/best_case_reorder.csv"}
                    #_:sink #_{:TheVoid "/Users/niko/data/results/joinorder/best_case_reorder.csv"}}}])))

  (exec! conn
    (let [name (name (gensym "best-case-hector"))]
      (println name)
      (concat
       (df/register
        db name
        {:Hector
         {:variables '[?post ?ip ?browser ?language ?content]
          :bindings  [(binding/attribute '[?post :post/ip ?ip])
                      (binding/attribute '[?post :post/browser ?browser])
                      (binding/attribute '[?post :post/language ?language])
                      (binding/attribute '[?post :post/content ?content])]}}
        [])
       [{:Interest {:name name
                    :sink {:TheVoid "/home/niko/results/joinorder/best_case_hector.csv"}
                    #_:sink #_{:TheVoid "/Users/niko/data/results/joinorder/best_case_hector.csv"}}}])))
  
  )



(comment

  ;; Evaluate intermediate join arrangements.

  (exec! conn
    (let [name (name (gensym "joinstate"))]
      (println name)
      (concat
       (df/register-query
        db name
        '[:find ?person
          :where
          [?person :person/firstname ?a]
          [?person :person/lastname ?b]
          ;; [?person :person/gender ?c]
          ;; [?person :person/birthday ?d]
          ;; [?person :person/creation-date ?e]
          ;; [?person :person/ip ?f]
          ;; [?person :person/browser ?g]
          ])
       [{:Interest {:name name
                    :sink {:TheVoid "/Users/niko/data/results/joinstate/join.csv"}}}])))

  (exec! conn
    (let [name (name (gensym "joinstate-hector"))]
      (concat
       (df/register
        db name
        {:Hector
         {:variables '[?person]
          :bindings  [(binding/attribute '[?person :person/firstname ?a])
                      (binding/attribute '[?person :person/lastname ?b])
                      (binding/attribute '[?person :person/gender ?c])
                      (binding/attribute '[?person :person/birthday ?d])
                      (binding/attribute '[?person :person/creation-date ?e])
                      ;; (binding/attribute '[?person :person/ip ?f])
                      ;; (binding/attribute '[?person :person/browser ?g])
                      ]}}
        [])
       [{:Interest {:name name
                    :sink {:TheVoid "/Users/niko/data/results/joinstate/delta.csv"}}}])))

  )



(comment

    ;; blows up
  (let [name (name (gensym "test"))]
    (exec! conn
      (df/register-query db name '[:find (count ?user)
                                   :where
                                   [?a :comment/place ?place-a]
                                   [?a :comment/creator ?user]
                                   [?b :comment/creator ?user]
                                   [?b :comment/place ?place-b]
                                   [?place-a :place/part-of ?country-a]
                                   [?country-a :place/name "Asia"]
                                   [?place-b :place/part-of ?country-b]
                                   [?country-b :place/name "Europe"]])
      (df/interest name)))

  (let [name (name (gensym "test"))]
    (exec! conn
      (df/register-query db name '[:find (count ?user)
                                   :where
                                   ;; a-ok here
                                   ;; [?param :input.selector/person ?user]
                                   
                                   [?a :comment/place ?place-a]
                                   [?place-a :place/part-of ?country-a]
                                   [?country-a :place/name "Asia"]
                                   [?a :comment/creator ?user]
                                   [?b :comment/creator ?user]

                                   ;; blows up here
                                   [?param :input.selector/person ?user]
                                   
                                   [?b :comment/place ?place-b]
                                   [?place-b :place/part-of ?country-b]
                                   [?country-b :place/name "Europe"]])
      (df/interest name)))

  
  (let [name (name (gensym "test"))]
    (exec! conn
      (df/register db name {:Hector
                            {:variables '[?user]
                             :bindings  [(binding/attribute '[?a :comment/place ?place-a])
                                         (binding/attribute '[?place-a :place/part-of ?country-a])
                                         (binding/attribute '[?country-a :place/name ?name-a])
                                         (binding/constant '[?name-a "Asia"])
                                         (binding/attribute '[?a :comment/creator ?user])
                                         (binding/attribute '[?b :comment/creator ?user])
                                         (binding/attribute '[?b :comment/place ?place-b])
                                         (binding/attribute '[?place-b :place/part-of ?country-b])
                                         (binding/attribute '[?country-b :place/name ?name-b])
                                         (binding/constant '[?name-b "Europe"])

                                         (binding/attribute '[?param :input.selector/person ?user])]}} [])
      (df/interest name)))

  )



;; Calculating memory blowup of a dataset

(defn normalization-tax
  "When normalizing tables of n entities and m (non-key) columns, we can
  expect a size increase of m * n * (8 + 8 + 8)byte (64bit for the
  eid, 64 for the timestamp, 64 for the diff)."
  [columns rows]
  (* 24 rows columns))

(comment
  (let [base  (* 214 1e6)
        taxes {:comment    (normalization-tax 4 632042)
               :forum      (normalization-tax 2 12380)
               :membership (normalization-tax 2 275908)
               :org        (normalization-tax 3 1253)
               :person     (normalization-tax 7 1000)
               :place      (normalization-tax 3 1073)
               :post       (normalization-tax 6 173401)
               :tag        (normalization-tax 2 1457)
               :tagclass   (normalization-tax 2 48)}
        total (apply + (vals taxes))]
    (-> (+ base total)
        (* 6) ;; indexing
        (/ 1e6)))

  ;; => we pay about 50MB extra for normalization.

  (let [base (* 20 1e6)
        tax  (normalization-tax 1 662890)]
    (-> (+ base tax)
        (* 6)
        (/ 1e6)))
  
  )
