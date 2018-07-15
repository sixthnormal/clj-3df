(ns clj-3df.parser
  (:refer-clojure :exclude [resolve])
  (:require
   [clojure.string :as str]
   [clojure.set :as set]
   #?(:clj [clojure.spec.alpha :as s]
      :cljs [cljs.spec.alpha :as s])))

;; CONFIGURATION

;; (timbre/merge-config! {:level :info})

(def log-level :info)

(defmacro info [& args]
  (apply list 'println args))

(defmacro trace [& args]
  (when (= log-level :trace)
    (apply list 'println args)))

;; UTIL

(def ^{:arglists '([pred] [pred coll])} separate (juxt filter remove))

(defn- pipe-log
  ([ctx] (pipe-log ctx "log:"))
  ([ctx message]
   (info message ctx)
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
        ::pred-expr (s/tuple (s/cat :predicate ::predicate
                                    :fn-args (s/+ (s/or :var ::variable
                                                        :const ::value))))
        ::lookup (s/tuple ::eid keyword? ::variable)
        ::entity (s/tuple ::eid ::variable ::variable)
        ::hasattr (s/tuple ::variable keyword? ::variable)
        ::filter (s/tuple ::variable keyword? ::value)
        ::rule-expr (s/cat :rule-name ::rule-name
                           :fn-args (s/+ (s/or :var ::variable
                                               :const ::value)))))

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
(s/def ::predicate '#{<= < > >= = not=})

(defn- is-var? [[tag _]] (= tag :var))

;; QUERY PLAN GENERATION

;; When resolving queries we have a single goal: determining a unique
;; binding for every logic variable. Therefore we need to specify how
;; to unify two competing bindings in all possible contexts
;; (e.g. 'and', 'or', 'not').

(s/def ::unification-method #{:unify/conjunction :unify/disjunction :unify/negation})

;; 1. Step: In the first pass the tree of (potentially) nested,
;; context-modifying operators is navigated and all clauses are
;; extracted into a flat list. Context information is preserverd by
;; tagging clauses.

(s/def ::tag (s/tuple ::unification-method number?))

(defrecord TaggingContext [clauses tag])
(defrecord TaggedClause [id tag type clause])

(defn- make-tagging-context
  ([] (make-tagging-context [[:unify/conjunction :root]]))
  ([root-tag] (TaggingContext. #{} root-tag)))

(defn- tagged-clause [ctx type clause]
  (TaggedClause. (. clojure.lang.RT (nextID)) (.-tag ctx) type clause))

(defn- generate-tag [method]
  [method (. clojure.lang.RT (nextID))])

(defmulti tag-clauses (fn [ctx clause] (first clause)))

(defmethod tag-clauses ::where [ctx [_ clauses]]
  (reduce tag-clauses ctx clauses))

(defmethod tag-clauses ::and [ctx [_ {:keys [clauses]}]]
  (as-> ctx nested
    (update nested :tag conj (generate-tag :unify/conjunction))
    (reduce tag-clauses nested clauses)
    (assoc ctx :clauses (.-clauses nested))))

(defmethod tag-clauses ::or [ctx [_ {:keys [clauses]}]]
  (as-> ctx nested
    (update nested :tag conj (generate-tag :unify/disjunction))
    (reduce tag-clauses nested clauses)
    (assoc ctx :clauses (.-clauses nested))))

(defmethod tag-clauses ::or-join [ctx [_ {:keys [symbols clauses]}]]
  (as-> ctx nested
    (update nested :tag conj (with-meta (generate-tag :unify/disjunction) {:projection symbols}))
    (reduce tag-clauses nested clauses)
    (assoc ctx :clauses (.-clauses nested))))

(defmethod tag-clauses ::not [ctx [_ {:keys [clauses]}]]
  (as-> ctx nested
    (update nested :tag conj (generate-tag :unify/negation))
    (reduce tag-clauses nested clauses)
    (assoc ctx :clauses (.-clauses nested))))

(defmethod tag-clauses ::pred-expr [ctx [_ predicate-expr]]
  (update ctx :clauses conj (tagged-clause ctx ::pred-expr predicate-expr)))

(defmethod tag-clauses ::lookup [ctx [_ clause]]
  (update ctx :clauses conj (tagged-clause ctx ::lookup clause)))

(defmethod tag-clauses ::entity [ctx [_ clause]]
  (update ctx :clauses conj (tagged-clause ctx ::entity clause)))

(defmethod tag-clauses ::hasattr [ctx [_ clause]]
  (update ctx :clauses conj (tagged-clause ctx ::hasattr clause)))

(defmethod tag-clauses ::filter [ctx [_ clause]]
  (update ctx :clauses conj (tagged-clause ctx ::filter clause)))

(defmethod tag-clauses ::rule-expr [ctx [_ clause]]
  (update ctx :clauses conj (tagged-clause ctx ::rule-expr clause)))

;; 2. Step: Now we walk the flat list of clauses and convert constant
;; bindings to inputs.

(defrecord NormalizationContext [clauses inputs])

(defn- make-normalization-context [tagging-ctx]
  (NormalizationContext. (.-clauses tagging-ctx) {}))

(defn- const->in [args]
  (->> args
       (reduce
        (fn [state arg]
          (if (is-var? arg)
            (update state :normalized-args conj (second arg))
            (let [in (gensym "?in_")]
              (-> state
                  (update :inputs assoc in arg)
                  (update :normalized-args conj in)))))
        {:inputs {} :normalized-args []})))

(defmulti normalize (fn [ctx clause] (.-type clause)))

(defmethod normalize ::pred-expr [ctx tagged]
  (let [[{:keys [predicate fn-args]}]    (.-clause tagged)
        {:keys [inputs normalized-args]} (const->in fn-args)]
    (as-> ctx ctx
      (update ctx :inputs merge inputs)
      (update ctx :clauses disj tagged)
      (update ctx :clauses conj (assoc tagged :clause [{:predicate predicate
                                                        :fn-args   normalized-args}])))))

;; actually handled in rust at the moment
(defmethod normalize ::lookup [ctx tagged] ctx)
(defmethod normalize ::entity [ctx tagged] ctx)
(defmethod normalize ::hasattr [ctx tagged] ctx)
(defmethod normalize ::filter [ctx tagged] ctx)

(defmethod normalize ::rule-expr [ctx tagged]
  (let [{:keys [rule-name fn-args]}      (.-clause tagged)
        {:keys [inputs normalized-args]} (const->in fn-args)]
    (as-> ctx ctx
      (update ctx :inputs merge inputs)
      (update ctx :clauses disj tagged)
      (update ctx :clauses conj (assoc tagged :clause {:rule-name rule-name
                                                       :fn-args   normalized-args})))))

;; 3. Step: Optimize clause order. @TODO

(defn optimize [clauses] (into [] clauses))

;; 4. Step: Sort clauses according to any dependencies between them,
;; ofcourse while attempting to preserve as much of the ordering from
;; the previous step. Dependencies occur, whenever a clause does not
;; produce bindings of its own, as is the case with ::pred-expr.

(defn reorder [clauses]
  (let [clauses       (->> clauses
                           (sort-by (fn [tagged] (conj (.-tag tagged) (.-id tagged))))
                           (reverse))
        ;; [preds other] (separate (fn [tagged] (= (.-type tagged) ::pred-expr)) clauses)
        ;clauses       (concat [] other preds)
        ]
    clauses))

;; 5. Step: Unification. We process the list until all conflicts are
;; resolved. Clauses are in conflict iff they share one or more
;; symbols. Conflicts must be resolved according to the unification
;; method of the most specific (i.e. nested) context they share.

(defrecord UnificationContext [symbols inputs attr->int relations deferred])
(defrecord Relation [tag symbols plan]
  Object
  (toString [this] (str "Rel " (mapv str symbols) " " plan)))

(defn- make-unification-context [db inputs]
  (UnificationContext. {} inputs (:attr->int db) #{} []))

(defn- resolve [ctx sym]
  (if-let [pair (find (.-symbols ctx) sym)]
    (val pair)
    (if-let [pair (find (.-inputs ctx) sym)]
      (val pair)
      (throw (ex-info "Unknown symbol." {:ctx ctx :sym sym})))))

(defn- resolve-all [ctx syms] (mapv #(resolve ctx %) syms))

(defn- attr-id [ctx a]
  (if-let [pair (find (:attr->int ctx) a)]
    (val pair)
    (throw (ex-info "Unknown attribute." {:ctx ctx :attr a}))))

(defn- render-value [[type v]]
  (case type
    :string {:String v}
    :number {:Number v}
    :bool   {:Bool v}))

(defn- extract-relation
  "Extracts the final remaining relation from a context. Will throw if
  more than one relation is still present."
  [^UnificationContext ctx]
  (if (> (count (.-relations ctx)) 1)
    (throw (ex-info "More than one relation present in context." ctx))
    (first (.-relations ctx))))

(defn- binds? [^Relation rel sym] (some #{sym} (.-symbols rel)))
(defn- binds-all? [^Relation rel syms] (set/subset? syms (set (.-symbols rel))))

(defn- shared-symbols [^Relation r1 ^Relation r2]
  (set/intersection (set (:symbols r1)) (set (:symbols r2))))

(defn- conflicting? [^Relation r1 ^Relation r2]
  (some? (seq (shared-symbols r1 r2))))

(defn- introduce-symbol [^UnificationContext ctx symbol]
  (if (contains? (.-symbols ctx) symbol)
    ctx
    (let [last-id (or (some->> (.-symbols ctx) (vals) (apply max)) -1)]
      (assoc-in ctx [:symbols symbol] (inc last-id)))))

(defn- shared-context
  "Identifies the most specific context shared by two tags."
  [tag1 tag2]
  (let [count (min (count tag1) (count tag2))
        pos   (dec count)]
    (cond
      (and (empty? tag1) (empty? tag2)) (throw (ex-info "Clauses must share a context." {:tag1 tag1 :tag2 tag2}))
      (= (nth tag1 pos) (nth tag2 pos)) (into [] (take count) tag1)
      :else                             (shared-context (take pos tag1) (take pos tag2)))))

;; (shared-context [[:unify/conjunction :root] [:unify/disjunction 25684] [:unify/conjunction 25685]]
;;                 [[:unify/conjunction :root] [:unify/disjunction 25684] ])
;; => [[:unify/conjunction :root] [:unify/disjunction 25684]]

(defn- join
  "Unifies two conflicting relations by equi-joining them."
  [^UnificationContext ctx r1 r2]
  (trace "joining" (:plan r1) (:plan r2))
  (let [shared      (shared-symbols r1 r2)
        ;; @TODO join on more than one variable
        join-sym    (first shared)
        result-syms (concat [join-sym] (remove shared (:symbols r1)) (remove shared (:symbols r2)))
        plan        {:Join [(:plan r1) (:plan r2) (resolve ctx join-sym)]}]
    (Relation. (shared-context (.-tag r1) (.-tag r2)) result-syms plan)))

(defn- antijoin
  "Unifies two conflicting relations by anti-joining them."
  [^UnificationContext ctx r1 r2]
  (trace "anti-joining" (:plan r1) (:plan r2))
  (let [shared      (shared-symbols r1 r2)
        join-sym    (first shared)
        result-syms (concat [join-sym] (remove shared (:symbols r1)) (remove shared (:symbols r2)))
        plan        {:Antijoin [(:plan r1) (:plan r2) (resolve ctx join-sym)]}]
    (Relation. (shared-context (.-tag r1) (.-tag r2)) result-syms plan)))

(defn- merge-unions [r1 r2]
  (let [[resolved-syms plans] (get-in r1 [:plan :Union])]
    (Relation. (shared-context (.-tag r1) (.-tag r2))
               (:symbols r1)
               {:Union [resolved-syms (conj plans (:plan r2))]})))

(defn- union
  "Unifies two conflicting relations by taking their union."
  [^UnificationContext ctx r1 r2]
  (trace "union of" (:plan r1) (:plan r2))
  (let [shared-ctx    (shared-context (.-tag r1) (.-tag r2))
        projection    (get (meta (last shared-ctx)) :projection)
        symbols       (if (some? projection) projection (:symbols r1))
        _             (when-not (and (binds-all? r1 symbols) (binds-all? r2 symbols))
                        (throw (ex-info "Relations must be union compatible inside of an or-clause. Insert suitable projections."
                                        {:r1 r1 :r2 r2})))
        resolved-syms (resolve-all ctx symbols)
        union?        (fn [rel] (= (get-in rel [:plan :Union 0]) resolved-syms))
        [r1 r2]       (cond
                        (and (union? r1)
                             (union? r2)) (throw (ex-info "Shouldn't be unifying two unions." {:r1 r1 :r2 r2}))
                        (union? r1)       [r1 r2]
                        (union? r2)       [r2 r1]
                        :else             [(Relation. shared-ctx
                                                      symbols
                                                      {:Union [resolved-syms [(:plan r1)]]}) r2])]
    (merge-unions r1 r2)))

(defn- project [^UnificationContext ctx symbols rel]
  (if (= (.-symbols rel) symbols)
    rel
    (Relation. (.-tag rel) symbols {:Project [(:plan rel) (resolve-all ctx symbols)]})))

(defn- introduceable?
  "Checks whether all conditions are met for a clause to be applied."
  [^UnificationContext ctx ^TaggedClause {:keys [type clause]}]
  (case type
    ::pred-expr (let [[{:keys [predicate fn-args]}] clause
                      deps                          (into #{} (remove (.-inputs ctx)) fn-args)]
                  (some #(binds-all? % deps) (.-relations ctx)))
    true))

(defn- plan-clause
  "Maps clauses to relations."
  [^UnificationContext ctx ^TaggedClause {:keys [type clause tag]}]
  (let [resolve  (partial resolve ctx)
        attr-id  (partial attr-id ctx)]
    (case type
      ::lookup    (let [[e a sym-v] clause]
                    (Relation. tag [sym-v] {:Lookup [e (attr-id a) (resolve sym-v)]}))
      ::entity    (let [[e sym-a sym-v] clause]
                    (Relation. tag [sym-a sym-v] {:Entity [e (resolve sym-a) (resolve sym-v)]}))
      ::hasattr   (let [[sym-e a sym-v] clause]
                    (Relation. tag [sym-e sym-v] {:HasAttr [(resolve sym-e) (attr-id a) (resolve sym-v)]}))
      ::filter    (let [[sym-e a v] clause]
                    (Relation. tag [sym-e] {:Filter [(resolve sym-e) (attr-id a) (render-value v)]}))
      ::rule-expr (let [{:keys [rule-name fn-args]} clause]
                    (Relation. tag fn-args {:RuleExpr [(str rule-name) (resolve-all ctx fn-args)]})))))

(defn- unify-with
  [^UnificationContext ctx ^TaggedClause tagged]
  (let [rel                (plan-clause ctx tagged)
        _                  (trace "unifying" (str rel))
        ;; for optimization purposes we do want to aggregate here, but
        ;; we mus be careful to aggregate without ignoring a more
        ;; specific context
        [conflicting free] (separate #(conflicting? rel %) (.-relations ctx))
        _                  (trace "conflicting" (mapv str conflicting))
        unified            (if (empty? conflicting)
                             rel
                             (reduce
                              (fn [rel conflicting]
                                (let [shared-ctx (shared-context (.-tag rel) (.-tag conflicting))
                                      [method _] (last shared-ctx)
                                      negated?   (fn [rel]
                                                   (true?
                                                    (->> (.-tag rel)
                                                         (drop (count shared-ctx))
                                                         (some (fn [[method _]] (= :unify/negation method))))))
                                      _          (trace "method" method (negated? rel) (negated? conflicting))
                                      rel'       (case [method (negated? rel) (negated? conflicting)]
                                                   [:unify/conjunction false false] (join ctx conflicting rel)
                                                   [:unify/conjunction false true]  (antijoin ctx rel conflicting)
                                                   [:unify/conjunction true false]  (antijoin ctx conflicting rel)
                                                   [:unify/disjunction false false] (union ctx conflicting rel)
                                                   [:unify/disjunction false true]  (throw (ex-info "Unbound not" {:rel rel :conflicting conflicting}))
                                                   [:unify/disjunction true false]  (throw (ex-info "Unbound not" {:rel rel :conflicting conflicting})))]
                                  rel'))
                              rel conflicting))]
    (trace "unified" (conj (set free) unified))
    (assoc ctx :relations (conj (set free) unified))))

(defmulti introduce-clause (fn [^UnificationContext ctx tagged] (.-type tagged)))

(defmethod introduce-clause ::lookup [ctx tagged]
  (let [[e a sym-v] (.-clause tagged)]
    (as-> ctx ctx
      (introduce-symbol ctx sym-v)
      (unify-with ctx tagged))))

(defmethod introduce-clause ::entity [ctx tagged]
  (let [[e sym-a sym-v] (.-clause tagged)]
    (as-> ctx ctx
      (introduce-symbol ctx sym-a)
      (introduce-symbol ctx sym-v)
      (unify-with ctx tagged))))

(defmethod introduce-clause ::hasattr [ctx tagged]
  (let [[sym-e a sym-v] (.-clause tagged)]
    (as-> ctx ctx
      (introduce-symbol ctx sym-e)
      (introduce-symbol ctx sym-v)
      (unify-with ctx tagged))))

(defmethod introduce-clause ::filter [ctx tagged]
  (let [[sym-e a v] (.-clause tagged)]
    (as-> ctx ctx
      (introduce-symbol ctx sym-e)
      (unify-with ctx tagged))))

(defmethod introduce-clause ::rule-expr [ctx tagged]
  (let [{:keys [rule-name fn-args]} (.-clause tagged)]
    (as-> ctx ctx
      (reduce introduce-symbol ctx fn-args)
      (unify-with ctx tagged))))

(defmethod introduce-clause ::pred-expr [ctx {:keys [clause tag]}]
  ;; assume for now, that input symbols are bound at this point
  (let [[{:keys [predicate fn-args]}] clause
        encode-predicate              {'< "LT" '<= "LTE" '> "GT" '>= "GTE" '= "EQ" 'not= "NEQ"}
        deps                          (into #{} (remove (.-inputs ctx)) fn-args)
        [matching other]              (separate #(binds-all? % deps) (.-relations ctx))]
    (if (not= (count matching) 1)
      (throw (ex-info "All predicate inputs must be bound in a single relation." {:predicate predicate
                                                                                  :deps      deps
                                                                                  :matching  matching
                                                                                  :other     other}))
      (let [rel     (first matching)
            wrap    (fn [plan] {:PredExpr [(encode-predicate predicate) (resolve-all ctx fn-args) plan]})
            wrapped (update rel :plan wrap)]
        (assoc ctx :relations (conj (set other) wrapped))))))

(defn- skip-clause [^UnificationContext ctx ^TaggedClause tagged]
  (trace "skipping" (.-type tagged))
  (update ctx :deferred conj tagged))

(defn unify [ctx clauses]
  (let [clauses         (reorder clauses)
        process-clauses (fn [ctx clauses]
                          (reduce (fn [ctx clause]
                                    (if (introduceable? ctx clause)
                                      (introduce-clause ctx clause)
                                      (skip-clause ctx clause))) ctx clauses))
        ctx             (process-clauses ctx clauses)]
    (if (empty? (.-deferred ctx))
      ctx
      (loop [ctx ctx clauses (.-deferred ctx)]
        (trace "Deferred clauses present, looping")
        (let [ctx' (process-clauses (assoc ctx :deferred []) clauses)]
          (cond
            (empty? (.-deferred ctx'))    ctx'
            (= (.-deferred ctx') clauses) (throw (ex-info "Un-introducable clauses" {:clauses (.-deferred ctx') :ctx ctx'}))
            :else                         (recur ctx' (.-deferred ctx'))))))))

;; 6. Step: Resolve find specification.

(defmulti impl-find (fn [ctx find-spec] (first find-spec)))

(defmethod impl-find ::find [ctx [_ find-spec]] (impl-find ctx find-spec))

(defmethod impl-find ::find-rel [ctx [_ syms]]
  (let [[bound unbound]       (separate (fn [sym] (some #(binds? % sym) (.-relations ctx))) (map second syms))
        relevant?             (fn [rel]
                                (some? (set/intersection (set (.-symbols rel)) (set bound))))
        [relevant irrelevant] (separate relevant? (.-relations ctx))]
    (cond
      (seq unbound)          (throw (ex-info "Find spec contains unbound symbols." {:unbound unbound :ctx ctx}))
      (empty? relevant)      (throw (ex-info "Find spec doesn't match any symbols." {:find-symbols syms}))
      (> (count relevant) 1) (throw (ex-info "Projecting across multiple relations is not yet supported." ctx))
      :else
      (as-> ctx ctx
        (assoc ctx :relations (set irrelevant))
        (update ctx :relations conj (project ctx bound (first relevant)))))))

;; PUBLIC API

(defrecord CompiledQuery [plan in])
(defrecord Rule [name plan])

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

(defn compile-query [db query]
  (let [ir                       (parse-query query)
        {:keys [clauses inputs]} (as-> (tag-clauses (make-tagging-context) [::where (:where ir)]) ctx
                                   (make-normalization-context ctx)
                                   (reduce normalize ctx (.-clauses ctx)))
        inputs                   (merge inputs (zipmap (:in ir) (map #(vector :input %) (range))))
        ordered-clauses          (->> clauses (optimize) (reorder))
        find-symbols             (->> (:find ir) second (mapv second))
        unification-ctx          (as-> (make-unification-context db inputs) ctx
                                   (reduce introduce-symbol ctx find-symbols)
                                   (unify ctx ordered-clauses)
                                   (impl-find ctx [::find (:find ir)]))
        plan                     (-> unification-ctx extract-relation :plan)]
    (CompiledQuery. plan inputs)))

(defn parse-rules [rules]
  (let [conformed (s/conform ::rules rules)]
    (if (s/invalid? conformed)
      (throw (ex-info "Couldn't parse rules" (s/explain-data ::rules rules)))
      conformed)))

(defn compile-rules [db rules]
  (let [ir                (parse-rules rules)
        get-head          #(get-in % [0 :head])
        by-head           (group-by get-head ir)

        ;; We perform a transformation here, wrapping body clauses
        ;; with (and) and rule definitions with (or). Note that :Union
        ;; with a single relation is equivalent to a :Project.
        get-clauses       #(get-in % [0 :clauses])
        wrap-and          (fn [clauses] [::and {:clauses clauses}])
        rewrite-rule      (fn [rewritten head rules]
                            (let [wrapped-clauses (map (comp wrap-and get-clauses) rules)]
                              (if (= (count rules) 1)
                                (assoc rewritten head [::where (get-clauses (first rules))])
                                (assoc rewritten head [::where [[::or-join {:symbols (:vars head)
                                                                            :clauses wrapped-clauses}]]]))))
        rewritten         (reduce-kv rewrite-rule {} by-head)
        
        compile-rewritten (fn [compiled head ir]
                            (let [{:keys [inputs clauses]}
                                  (as-> (tag-clauses (make-tagging-context) ir) ctx
                                    (make-normalization-context ctx)
                                    (reduce normalize ctx (.-clauses ctx)))

                                  ordered-clauses
                                  (->> clauses (optimize) (reorder))
                                  
                                  unification-ctx
                                  (as-> (make-unification-context db inputs) ctx
                                    (reduce introduce-symbol ctx (:vars head))
                                    (unify ctx ordered-clauses))]
                              (assoc compiled head unification-ctx)))
        compiled  (reduce-kv compile-rewritten {} rewritten)

        rel->rule (fn [rules head ctx]
                    (let [rule-name (str (:name head))
                          plan      (->> ctx extract-relation (project ctx (:vars head)) :plan)]
                      (conj rules (Rule. rule-name plan))))
        rules     (reduce-kv rel->rule #{} compiled)]
    rules))

(comment
  (compile-rules {:attr->int {:admin? 100}} '[[(admin? ?user) [?user :admin? true]]])

  (compile-rules {:attr->int {:node 100 :edge 200}}
                 '[[(propagate ?x ?y) [?x :node ?y]]
                   [(propagate ?x ?y) [?z :edge ?y] (propagate ?x ?z)]])

  (let [q0 '[:find ?t1 ?t2
             :where
             [?op :time ?t1]
             [?op :time ?t2]
             [(< ?t2 100)]
             (or [(< ?t1 ?t2)]
                 [(< ?t2 ?t1)]
                 (and [(= ?t1 ?t2)]
                      (not [?op2 :time ?t2])
                      (some-rule ?t1 ?t2)
                      (another-rule ?op "ADD")))]
        q1 '[:find ?op ?op2
             :where
             [?op :time ?t]
             [?op2 :time ?t]]
        q2 '[:find ?t1 ?t2
             :where
             [?op :time ?t1]
             [?op2 :time ?t2]
             (yarule ?t1 ?t2)
             [(< ?t1 ?t2)]]]
    (compile-query {:attr->int {:time 100}} q2)))

