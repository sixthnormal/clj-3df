(ns clj-3df.parser
  (:refer-clojure :exclude [resolve])
  (:require
   [clojure.string :as str]
   [clojure.set :as set]
   #?(:clj [clojure.spec.alpha :as s]
      :cljs [cljs.spec.alpha :as s])))

;; CONFIGURATION

(def debug? false)

;; UTIL

(def ^{:arglists '([pred] [pred coll])} separate (juxt filter remove))

(defn- log
  ([ctx] (log ctx "log:"))
  ([ctx message]
   (println message ctx)
   ctx))

;; GRAMMAR

(s/def ::query (s/keys :req-un [::find ::where]
                       :opt-un [::in]))

(s/def ::find (s/alt ::find-rel ::find-rel))
(s/def ::find-rel (s/+ ::find-elem))
(s/def ::find-elem (s/or :var ::variable))

(s/def ::in (s/+ ::variable))

(s/def ::where (s/+ ::clause))

(s/def ::clause
  (s/or ::and (s/cat :marker #{'and} :clauses (s/+ ::clause))
        ::or (s/cat :marker #{'or} :clauses (s/+ ::clause))
        ::or-join (s/cat :marker #{'or-join} :symbols (s/and vector? (s/+ ::variable)) :clauses (s/+ ::clause))
        ::not (s/cat :marker #{'not} :clauses (s/+ ::clause))
        ::lookup (s/tuple ::eid keyword? ::variable)
        ::entity (s/tuple ::eid ::variable ::variable)
        ::hasattr (s/tuple ::variable keyword? ::variable)
        ::filter (s/tuple ::variable keyword? ::value)
        ::rule-expr (s/cat :rule-name ::rule-name
                           :symbols (s/+ ::variable))))

(s/def ::rules (s/and vector? (s/+ ::rule)))
(s/def ::rule (s/and vector?
                     (s/+ (s/cat :head ::rule-head
                                 :clauses (s/+ ::clause)))))
(s/def ::rule-head (s/and list? (s/cat :name ::rule-name :vars (s/+ ::variable))))
(s/def ::rule-name (s/and symbol? #(-> % name (str/starts-with? "?") (not))))

(s/def ::eid number?)
;; (s/def ::symbol (s/or ::placeholder #{'_}
;;                       ::variable ::variable))
(s/def ::variable (s/and symbol?
                         #(-> % name (str/starts-with? "?"))))
(s/def ::value (s/or :number number?
                     :string string?
                     :bool   boolean?))

;; QUERY PLAN GENERATION

(defrecord Context [attrs syms rels operator negate?])
(defrecord Relation [symbols plan])
(defrecord Rule [name plan])

(defn- resolve [ctx sym] (get-in ctx [:syms sym]))
(defn- resolve-all [ctx syms] (mapv #(resolve ctx %) syms))

(defn- attr-id [ctx a] (get-in ctx [:attr->int a]))

(defn- render-value [[type v]]
  (case type
    :string {:String v}
    :number {:Number v}
    :bool   {:Bool v}))

(defn- extract-relation
  "Extracts the final remaining relation from a context. Will throw if
  more than one relation is still present."
  [ctx]
  (if (> (count (:rels ctx)) 1)
    (throw (ex-info "More than one relation present in context." ctx))
    (first (:rels ctx))))

(defn- negate-ctx [ctx]
  (if (:negate? ctx)
    (assoc ctx :negate? false)
    (assoc ctx :negate? true)))

(defn- introduce-sym [ctx sym]
  (if (contains? (:syms ctx) sym)
    ctx
    (let [last-id (or (some->> (:syms ctx) (vals) (apply max)) -1)]
      (assoc-in ctx [:syms sym] (inc last-id)))))

(defn- shared-symbols [r1 r2]
  (set/intersection (set (:symbols r1)) (set (:symbols r2))))

(defn- join
  "Unifies two conflicting relations by equi-joining them."
  [ctx r1 r2]
  (when debug?
    (println "Introducing join" (select-keys ctx [:rels :operator])))
  (let [shared      (shared-symbols r1 r2)
        ;; @TODO join on more than one variable
        join-sym    (first shared)
        result-syms (concat [join-sym] (remove shared (:symbols r1)) (remove shared (:symbols r2)))
        plan        {:Join [(:plan r1) (:plan r2) (resolve ctx join-sym)]}]
    (Relation. result-syms plan)))

(defn- merge-unions [r1 r2]
  (def test-r1 r1)
  (def test-r2 r2)
  (let [[resolved-syms plans] (get-in r1 [:plan :Union])]
    (Relation. (:symbols r1)
               {:Union [resolved-syms (conj plans (:plan r2))]})))

(defn- union
  "Unifies two conflicting relations by taking their union."
  ([ctx r1 r2]
   (cond
     ;; @TODO make this nicer
     (contains? ctx :or-join) (union ctx (:or-join ctx) r1 r2)
     
     (not= (:symbols r1) (:symbols r2))
     (throw (ex-info "Symbols must match inside of an or-clause. Use or-join instead."
                     {:r1-symbols (:symbols r1) :r2-symbols (:symbols r2)}))

     :else (union ctx (:symbols r1) r1 r2)))
  ([ctx symbols r1 r2]
   (when debug?
     (println "Introducing union" (select-keys ctx [:rels :operator])))
   (let [resolved-syms  (resolve-all ctx symbols)
         union?         (fn [rel] (= (get-in rel [:plan :Union 0]) resolved-syms))
         [r1 r2]        (cond
                          (and (union? r1) (union? r2)) (throw (ex-info "Shouldn't be unifying two unions." {:r1 r1 :r2 r2}))
                          (union? r1)                   [r1 r2]
                          (union? r2)                   [r2 r1]
                          :else                         [(Relation. symbols {:Union [resolved-syms [(:plan r1)]]}) r2])]
     (merge-unions r1 r2))))

(defn- introduce-relation [ctx rel]
  (when debug?
    (println "Introducing" (:plan rel) (select-keys ctx [:rels :operator])))
  (let [conflict?          (fn [other] (some? (shared-symbols rel other)))
        [conflicting free] (separate conflict? (:rels ctx))]
    ;; there can only ever be at most a single conflict (we should be
    ;; introducing relations one by one)
    (cond
      (empty? conflicting)      (update ctx :rels conj rel)
      (= (count conflicting) 1) (case (:operator ctx)
                                  :AND (assoc ctx :rels (conj free (join ctx (first conflicting) rel)))
                                  :OR  (assoc ctx :rels (conj free (union ctx (first conflicting) rel))))
      :else                     (throw (ex-info "More than one conflict found" {:ctx ctx :rel rel})))))

(defn- introduce-simple-relation [ctx rel]
  (if (:negate? ctx)
    (introduce-relation ctx (update rel :plan (fn [plan] {:Not plan})))
    (introduce-relation ctx rel)))

(defn- merge-child-context [parent child]
  (as-> parent ctx
    (update ctx :syms merge (:syms child))
    (reduce introduce-relation ctx (:rels child))))

(defn- project [ctx rel symbols]
  (if (= (:symbols rel) symbols)
    rel
    (Relation. symbols {:Project [(:plan rel) (resolve-all ctx symbols)]})))

(defmulti impl (fn [ctx query] (first query)))

(defmethod impl ::query [ctx [_ {:keys [find where] :as query}]]
  (-> ctx
      (impl [::where where])
      (impl [::find find])))

(defmethod impl ::find [ctx [_ find-spec]] (impl ctx find-spec))

(defmethod impl ::find-rel [ctx [_ syms]]
  (let [[bound unbound]       (separate (:syms ctx) (map second syms))
        relevant?             (fn [rel] (some? (set/intersection (set (:symbols rel)) (set bound))))
        [relevant irrelevant] (separate relevant? (:rels ctx))]
    (cond
      (seq unbound)          (throw (ex-info "Find spec contains unbound symbols." unbound))
      (empty? relevant)      (throw (ex-info "Find spec doesn't match any symbols." {:find-symbols syms}))
      (> (count relevant) 1) (throw (ex-info "Projecting across multiple relations is not yet supported." ctx))
      :else
      (as-> ctx ctx
        (assoc ctx :rels irrelevant)
        (update ctx :rels conj (project ctx (first relevant) bound))))))

(defmethod impl ::where [ctx [_ clauses]]
  (reduce impl ctx clauses))

(defmethod impl ::and [ctx [_ {:keys [clauses]}]]
  (when debug? (println "---- FRESH CONTEXT (AND) ---"))
  (let [child (reduce impl (assoc ctx
                                  :operator (if (:negate? ctx) :OR :AND)
                                  :rels #{}) clauses)]
    (merge-child-context ctx child)))

(defmethod impl ::or [ctx [_ {:keys [clauses]}]]
  (when debug? (println "---- FRESH CONTEXT (OR) ---"))
  (let [child (reduce impl (assoc ctx
                                  :operator (if (:negate? ctx) :AND :OR)
                                  :rels #{}) clauses)]
    (merge-child-context ctx child)))

(defmethod impl ::or-join [ctx [_ {:keys [symbols clauses]}]]
  (when debug? (println "---- FRESH CONTEXT (OR-JOIN) ---"))
  (let [child (reduce impl (assoc ctx
                                  :operator (if (:negate? ctx) :AND :OR)
                                  :or-join symbols
                                  :rels #{}) clauses)]
    (merge-child-context ctx child)))

(defmethod impl ::not [ctx [_ {:keys [clauses]}]]
  (-> (reduce impl (negate-ctx ctx) clauses)
      (assoc :negate? (:negate? ctx))))

(defmethod impl ::lookup [ctx [_ [e a sym-v]]]
  (as-> ctx ctx
    (introduce-sym ctx sym-v)
    (introduce-simple-relation ctx
                               (Relation. [sym-v] {:Lookup [e (attr-id ctx a) (resolve ctx sym-v)]}))))

(defmethod impl ::entity [ctx [_ [e sym-a sym-v]]]
  (as-> ctx ctx
    (introduce-sym ctx sym-a)
    (introduce-sym ctx sym-v)
    (introduce-simple-relation ctx
                               (Relation. [sym-a sym-v] {:Entity [e (resolve ctx sym-a) (resolve ctx sym-v)]}))))

(defmethod impl ::hasattr [ctx [_ [sym-e a sym-v]]]
  (as-> ctx ctx
    (introduce-sym ctx sym-e)
    (introduce-sym ctx sym-v)
    (introduce-simple-relation ctx
                               (Relation. [sym-e sym-v] {:HasAttr [(resolve ctx sym-e) (attr-id ctx a) (resolve ctx sym-v)]}))))

(defmethod impl ::filter [ctx [_ [sym-e a v]]]
  (as-> ctx ctx
    (introduce-sym ctx sym-e)
    (introduce-simple-relation ctx
                               (Relation. [sym-e] {:Filter [(resolve ctx sym-e) (attr-id ctx a) (render-value v)]}))))

(defmethod impl ::rule-expr [ctx [_ {:keys [rule-name symbols]}]]
  (introduce-relation ctx (Relation. symbols {:RuleExpr [(str rule-name) (resolve-all ctx symbols)]})))

(defmethod impl ::rules [ctx [_ rules]]
  (let [get-head     #(get-in % [0 :head])
        get-clauses  #(get-in % [0 :clauses])
        by-head      (group-by get-head rules)
        ;; We perform a transformation here, wrapping body clauses
        ;; with (and) and rule definitions with (or). Note that :Union
        ;; with a single relation is equivalent to a :Project.
        process-rule (fn [ctx [head rules]]
                       (let [rel->rule       (fn [rel] (Rule. (str (:name head)) (.-plan rel)))
                             wrap-and        (fn [clauses] [::and {:clauses clauses}])
                             wrapped-clauses (map (comp wrap-and get-clauses) rules)]
                         (as-> ctx ctx
                           (if (= (count rules) 1)
                             (let [ctx (impl ctx [::where (get-clauses (first rules))])
                                   rel (extract-relation ctx)]
                               (assoc ctx :rels #{(project ctx rel (:vars head))}))
                             (impl ctx [::or-join {:symbols (:vars head) :clauses wrapped-clauses}]))
                           (update ctx :rules into (map rel->rule (:rels ctx))))))]
    (reduce process-rule ctx (seq by-head))))

;; PUBLIC API

(defn query->map [query]
  (loop [parsed {}, key nil, qs query]
    (if-let [q (first qs)]
      (if (keyword? q)
        (recur parsed q (next qs))
        (recur (update-in parsed [key] (fnil conj []) q) key (next qs)))
      parsed)))

(defn parse-query [query]
  (let [query     (if (sequential? query) (query->map query) query)
        conformed (s/conform ::query query)]
    (if (s/invalid? conformed)
      (throw (ex-info "Couldn't parse query" (s/explain-data ::query query)))
      conformed)))

(defn plan-query [db query]
  (let [ctx-in  (map->Context {:attr->int (:attr->int db)
                               :syms      {}
                               :rels      #{}
                               :operator  :AND
                               :negate?   false})
        ir      (parse-query query)
        ctx-out (impl ctx-in [::query ir])]
    (-> ctx-out extract-relation :plan)))

(defn parse-rules [rules]
  (let [conformed (s/conform ::rules rules)]
    (if (s/invalid? conformed)
      (throw (ex-info "Couldn't parse rules" (s/explain-data ::rules rules)))
      conformed)))

(defn plan-rules [db rules]
  (let [ctx-in  (map->Context {:attr->int (:attr->int db)
                               :syms      {}
                               :rels      #{}
                               :rules     #{}
                               :operator  :AND
                               :negate?   false})
        ir      (parse-rules rules)
        ctx-out (impl ctx-in [::rules ir])]
    (-> ctx-out :rules)))
