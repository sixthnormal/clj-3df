(ns clj-3df.core
  (:refer-clojure :exclude [resolve])
  (:require
   [clojure.string :as str]
   [clojure.set :as set]
   #?(:clj [clojure.spec.alpha :as s]
      :cljs [cljs.spec.alpha :as s])))

;; CONFIGURATION

(def debug? false)

;; UTIL

(def separate (juxt filter remove))

(defn- log [ctx message]
  (println message ctx)
  ctx)

;; GRAMMAR

(s/def ::eid number?)
;; (s/def ::symbol (s/or ::placeholder #{'_}
;;                       ::variable ::variable))
(s/def ::variable (s/and symbol?
                         #(-> % name (str/starts-with? "?"))))
(s/def ::value (s/or :number number?
                     :string string?))

(s/def ::query (s/cat :name-clause (s/? (s/cat :name-marker #{:name} :name string?))
                      :find-marker #{:find}
                      :find ::find
                      :where-marker #{:where}
                      :where ::where))

(s/def ::find (s/alt ::find-rel ::find-rel))
(s/def ::find-rel (s/+ ::find-elem))
(s/def ::find-elem (s/or :var ::variable))

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
        ::recur (s/cat :marker #{'recur} :symbols (s/+ ::variable))))

;; QUERY PLAN GENERATION

(defrecord Context [attrs syms rels operator negate?])
(defrecord Relation [symbols plan])

(defn- resolve [ctx sym] (get-in ctx [:syms sym]))

(defn- attr-id [ctx a] (get-in ctx [:attr->int a]))

(defn- render-value [[type v]]
  (case type
    :string {:String v}
    :number {:Number v}))

(defn- negate-ctx [ctx]
  (if (:negate? ctx)
    (assoc ctx :negate? false)
    (assoc ctx :negate? true)))

(defn- introduce-sym [ctx sym]
  (if (contains? (:syms ctx) sym)
    ctx
    (let [last-id (or (some->> (:syms ctx) (vals) (apply max)) -1)]
      (assoc-in ctx [:syms sym] (inc last-id)))))

(defn- introduce-join
  "Unifies two conflicting relations by equi-joining them."
  [ctx [r1 r2]]
  (when debug?
    (println "Introducing join" (select-keys ctx [:rels :operator])))
  (let [;; @TODO join on more than one variable
        join-sym    (first (set/intersection (:symbols r1) (:symbols r2)))
        result-syms (set/union (:symbols r1) (:symbols r2))
        plan        {:Join [(:plan r1) (:plan r2) (resolve ctx join-sym)]}]
    (update ctx :rels conj (Relation. result-syms plan))))

(defn- introduce-union
  "Unifies two conflicting relations by taking their union."
  ([ctx [r1 r2]]
   (cond
     ;; @TODO make this nicer
     (contains? ctx :or-join) (introduce-union ctx (:or-join ctx) [r1 r2])
     
     (not= (:symbols r1) (:symbols r2))
     (throw (ex-info "Symbols must match inside of an or-clause. Use or-join instead."
                     {:r1-symbols (:symbols r1) :r2-symbols (:symbols r2)}))

     :else (introduce-union ctx (:symbols r1) [r1 r2])))
  ([ctx symbols [r1 r2]]
   (when debug?
     (println "Introducing union" (select-keys ctx [:rels :operator])))
   (let [plan {:Union [(:plan r1) (:plan r2) (mapv #(resolve ctx %) symbols)]}]
     (update ctx :rels conj (Relation. symbols plan)))))

(defn- introduce-relation [ctx rel]
  (when debug?
    (println "Introducing" (:plan rel) (select-keys ctx [:rels :operator])))
  (let [conflict?          (fn [other] (some? (set/intersection (:symbols rel) (:symbols other))))
        [conflicting free] (separate conflict? (:rels ctx))
        conflicting-pairs  (map vector (repeat rel) conflicting)
        on-conflict        (case (:operator ctx)
                             :AND introduce-join
                             :OR  introduce-union)]
    (if (empty? conflicting)
      (update ctx :rels conj rel)
      (as-> ctx ctx
        (assoc ctx :rels free)
        (reduce on-conflict ctx conflicting-pairs)))))

(defn- introduce-simple-relation [ctx rel]
  (if (:negate? ctx)
    (introduce-relation ctx (update rel :plan (fn [plan] {:Not plan})))
    (introduce-relation ctx rel)))

(defn- merge-child-context [parent child]
  (as-> parent ctx
    (update ctx :syms merge (:syms child))
    (reduce introduce-relation ctx (:rels child))))

(defmulti impl (fn [ctx query] (first query)))

(defmethod impl ::query [ctx [_ {:keys [find where] :as query}]]
  (-> ctx
      (impl [::where where])
      (impl [::find find])))

(defmethod impl ::find [ctx [_ find-spec]] (impl ctx find-spec))

(defmethod impl ::find-rel [ctx [_ syms]]
  (let [[bound unbound]       (separate (:syms ctx) (map second syms))
        relevant?             (fn [rel] (some? (set/intersection (:symbols rel) (set bound))))
        [relevant irrelevant] (separate relevant? (:rels ctx))]
    (cond
      (seq unbound)          (throw (ex-info "Find spec contains unbound symbols." unbound))
      (empty? relevant)      (throw (ex-info "Find spec doesn't match any symbols." {:find-symbols syms}))
      (> (count relevant) 1) (throw (ex-info "Projecting across multiple relations is not yet supported." ctx))
      :else
      (as-> ctx ctx
        (assoc ctx :rels irrelevant)
        (update ctx :rels conj (Relation. (set bound)
                                          {:Project [(:plan (first relevant)) (mapv #(resolve ctx %) bound)]}))))))

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
                               (Relation. #{sym-v} {:Lookup [e (attr-id ctx a) (resolve ctx sym-v)]}))))

(defmethod impl ::entity [ctx [_ [e sym-a sym-v]]]
  (as-> ctx ctx
    (introduce-sym ctx sym-a)
    (introduce-sym ctx sym-v)
    (introduce-simple-relation ctx
                               (Relation. #{sym-a sym-v} {:Entity [e (resolve ctx sym-a) (resolve ctx sym-v)]}))))

(defmethod impl ::hasattr [ctx [_ [sym-e a sym-v]]]
  (as-> ctx ctx
    (introduce-sym ctx sym-e)
    (introduce-sym ctx sym-v)
    (introduce-simple-relation ctx
                               (Relation. #{sym-e sym-v} {:HasAttr [(resolve ctx sym-e) (attr-id ctx a) (resolve ctx sym-v)]}))))

(defmethod impl ::filter [ctx [_ [sym-e a v]]]
  (as-> ctx ctx
    (introduce-sym ctx sym-e)
    (introduce-simple-relation ctx
                               (Relation. #{sym-e} {:Filter [(resolve ctx sym-e) (attr-id ctx a) (render-value v)]}))))

(defmethod impl ::recur [ctx [_ {:keys [symbols]}]]
  (introduce-relation ctx (Relation. (set symbols) {:Recur (mapv #(resolve ctx %) symbols)})))

;; PUBLIC API

(defrecord Differential [schema attr->int int->attr next-tx impl registrations])

(defn create-db [schema]
  (let [attr->int (zipmap (keys schema) (iterate (partial + 100) 100))
        int->attr (set/map-invert attr->int)]
    (Differential. schema attr->int int->attr 0 nil {})))

(defn parse [query]
  (let [conformed (s/conform ::query query)]
    (if (= conformed :cljs.spec.alpha/invalid)
      (throw (ex-info "Couldn't parse query" (s/explain-data ::query query)))
      conformed)))

(defn plan-query [db query]
  (let [ctx-in  (map->Context {:attr->int (:attr->int db)
                               :syms      {}
                               :rels      #{}
                               :operator  :AND
                               :negate?   false})
        ir      (parse query)
        ctx-out (impl ctx-in [::query ir])]
    (-> ctx-out :rels (first) :plan)))
