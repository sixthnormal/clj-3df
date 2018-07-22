(ns clj-3df.compiler
  (:require
   [clojure.string :as str]
   [clojure.set :as set]
   #?(:clj [clojure.spec.alpha :as s]
      :cljs [cljs.spec.alpha :as s])))

;; UTIL

(def ^:dynamic debug? false)
(def log-level :info)

(defmacro info [& args]
  `(println ~@args))

(defmacro trace [& args]
  (when (= log-level :trace)
    `(println ~@args)))

(defmacro trace-bindings [msg bindings]
  (when (= log-level :trace)
    `(do (println ~msg)
         (clojure.pprint/pprint (mapv plan ~bindings)))))

(defn- pipe-log
  ([ctx] (pipe-log ctx "log:"))
  ([ctx message]
   (info message ctx)
   ctx))

(def ^{:arglists '([pred] [pred coll])} separate (juxt filter remove))

;; GRAMMAR

(s/def ::query (s/keys :req-un [::find ::where]
                       :opt-un [::in]))

(s/def ::find (s/alt ::find-rel ::find-rel))
(s/def ::find-rel (s/+ ::find-elem))
(s/def ::find-elem (s/or :var ::variable :aggregate ::aggregate))

(s/def ::in (s/+ ::variable))

(s/def ::where (s/+ ::clause))

(s/def ::clause
  (s/or ::and (s/cat :marker #{'and} :clauses (s/+ ::clause))
        ::or (s/cat :marker #{'or} :clauses (s/+ ::clause))
        ::or-join (s/cat :marker #{'or-join} :symbols (s/and vector? (s/+ ::variable)) :clauses (s/+ ::clause))
        ::not (s/cat :marker #{'not} :clauses (s/+ ::clause))
        ::pred-expr (s/tuple (s/cat :predicate ::predicate :fn-args (s/+ ::fn-arg)))
        ::lookup (s/tuple ::eid keyword? ::variable)
        ::entity (s/tuple ::eid ::variable ::variable)
        ::hasattr (s/tuple ::variable keyword? ::variable)
        ::filter (s/tuple ::variable keyword? ::value)
        ::rule-expr (s/cat :rule-name ::rule-name :fn-args (s/+ ::fn-arg))))

(s/def ::aggregate (s/cat :aggregation-fn ::aggregation-fn :fn-args (s/+ ::fn-arg)))

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
(s/def ::aggregation-fn '#{min max count})
(s/def ::fn-arg (s/or :var ::variable :const ::value))

;; PARSING

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

(defn parse-rules [rules]
  (let [conformed (s/conform ::rules rules)]
    (if (s/invalid? conformed)
      (throw (ex-info "Couldn't parse rules" (s/explain-data ::rules rules)))
      conformed)))

;; QUERY PLAN GENERATION

;; When resolving queries we have a single goal: determining a unique
;; binding for every logic variable. Therefore we need to specify how
;; to unify two competing bindings in all possible contexts
;; (e.g. 'and', 'or', 'not').

(defprotocol IBinding
  (bound-symbols [binding] "Returns the ordered sequence of symbols bound by this binding.")
  (conflict? [binding other] "Returns true iff the 'other' binding conflicts with this binding.")
  (plan [binding] "Returns a query plan to compute this binding."))

(defn- debug-plan [b]
  (binding [debug? true] (plan b)))

(defn- binds?
  "Returns true iff the binding binds the specified symbol."
  [binding symbol]
  (some #{symbol} (bound-symbols binding)))

(defn- binds-all?
  "Returns true iff the binding binds all of the specified symbols."
  [binding symbols]
  (every? #(binds? binding %) symbols))

(defn- shared-symbols
  "Returns the set of symbols that is bound by both bindings."
  [& bindings]
  (apply set/intersection (map (comp set bound-symbols) bindings)))

;; Some clauses (inputs, data patterns and rule expressions) generate
;; bindings by themselves, we call them relations.

(defrecord Input [symbol value]
  IBinding
  (bound-symbols [this] [(.-symbol this)])
  (plan [this] {:Input (bound-symbols this)}))

(defrecord Relation [type pattern symbols]
  IBinding
  (bound-symbols [rel] (.-symbols rel))
  (plan [{:keys [type pattern]}]
    (let [encode-value (fn [v]
                         (cond
                           (string? v)  {:String v}
                           (number? v)  {:Number v}
                           (boolean? v) {:Bool v}))]
      (case type
        ::lookup  (let [[e a sym-v] pattern] {:Lookup [e a sym-v]})
        ::entity  (let [[e sym-a sym-v] pattern] {:Entity [e sym-a sym-v]})
        ::hasattr (let [[sym-e a sym-v] pattern] {:HasAttr [sym-e a sym-v]})
        ::filter  (let [[sym-e a [_ v]] pattern] {:Filter [sym-e a (encode-value v)]})))))

(defrecord RuleExpr [rule-name symbols]
  IBinding
  (bound-symbols [this] symbols)
  (plan [this] {:RuleExpr [(str rule-name) (bound-symbols this)]}))

;; Predicate epxressions, aggregations, and projections act on
;; existing bindings.

(defrecord Predicate [predicate args binding]
  IBinding
  (bound-symbols [this]
    (if (some? binding) (bound-symbols binding) args))
  (plan [this]
    (let [encode-predicate {'< "LT" '<= "LTE" '> "GT" '>= "GTE" '= "EQ" 'not= "NEQ"}
          symbols          (bound-symbols this)]
      (if (some? binding)
        {:PredExpr [(encode-predicate predicate) args (plan binding)]}
        (if debug?
          {:PredExpr [(encode-predicate predicate) args :_]}
          (throw (ex-info "All predicate inputs must be bound in a single relation." {:binding this})))))))

(defrecord Aggregation [fn-symbol args binding]
  IBinding
  (bound-symbols [this]
    (if (some? binding) (bound-symbols binding) args))
  (plan [this]
    (if (some? binding)
      {:Aggregate [(str/upper-case (name fn-symbol)) (plan binding) args]}
      (if debug?
        {:Aggregate [(str/upper-case (name fn-symbol)) :_ args]}
        (throw (ex-info "All aggregate arguments must be bound by a single relation." {:binding this}))))))

(defrecord Projection [symbols binding]
  IBinding
  (bound-symbols [this] symbols)
  (plan [this]
    (let [symbols (bound-symbols this)]
      (if (some? binding)
        (cond
          (= symbols (bound-symbols binding)) (plan binding)
          (binds-all? binding symbols)        {:Project [(plan binding) symbols]}
          debug?                              {:Project [(plan binding) symbols]})
        (if debug?
          {:Project [:_ symbols]}
          (throw (ex-info "Projection on unbound symbols." {:binding (debug-plan this)})))))))

;; In the first pass the tree of (potentially) nested,
;; context-modifying operators is navigated and all bindings are
;; brought into a form that we can work with. This mostly means
;; creating the right binding representation and transforming
;; constants into input bindings.

(defrecord Context [bindings op children])

(defn- const->in
  "Transforms any constant arguments into inputs."
  [args]
  (->> args
       (reduce
        (fn [state [typ arg]]
          (case typ
            :var   (update state :normalized-args conj arg)
            :const (let [in (gensym "?in_")]
                     (-> state
                         (update :inputs conj (->Input in [:const arg]))
                         (update :normalized-args conj in)))))
        {:inputs #{} :normalized-args []})))

(defmulti normalize (fn [^Context ctx clause] (first clause)))

(defmethod normalize ::and [ctx [_ {:keys [clauses]}]]
  (let [nested (reduce normalize (->Context #{} ::and []) clauses)]
    (update ctx :children conj nested)))

(defmethod normalize ::or [ctx [_ {:keys [clauses]}]]
  (let [nested (reduce normalize (->Context #{} ::or []) clauses)]
    (update ctx :children conj nested)))

(defmethod normalize ::or-join [ctx [_ {:keys [symbols clauses]}]]
  (let [nested (reduce normalize (->Context #{} ::or []) clauses)]
    (update ctx :children conj nested)))

(defmethod normalize ::not [ctx [_ {:keys [clauses]}]]
  (let [nested (reduce normalize (->Context #{} ::and []) clauses)
        nested (update nested :bindings (fn [bindings] (into #{} (map #(assoc % :negated? true)) bindings)))]
    (update ctx :children conj nested)))

(defmethod normalize ::pred-expr [ctx [_ predicate-expr]]
  (let [[{:keys [predicate fn-args]}]    predicate-expr
        {:keys [inputs normalized-args]} (const->in fn-args)]
    (-> ctx
        (update :bindings into inputs)
        (update :bindings conj (->Predicate predicate normalized-args nil)))))

(defmethod normalize ::lookup [ctx [_ [e a sym-v :as pattern]]]
  (update ctx :bindings conj (->Relation ::lookup pattern [sym-v])))

(defmethod normalize ::entity [ctx [_ [e sym-a sym-v :as pattern]]]
  (update ctx :bindings conj (->Relation ::entity pattern [sym-a sym-v])))

(defmethod normalize ::hasattr [ctx [_ [sym-e a sym-v :as pattern]]]
  (update ctx :bindings conj (->Relation ::hasattr pattern [sym-e sym-v])))

(defmethod normalize ::filter [ctx [_ [sym-e a v :as pattern]]]
  (update ctx :bindings conj (->Relation ::filter pattern [sym-e])))

(defmethod normalize ::rule-expr [ctx [_ rule-expr]]
  (let [{:keys [rule-name fn-args]}      rule-expr
        {:keys [inputs normalized-args]} (const->in fn-args)]
    (-> ctx
        (update :inputs into inputs)
        (update :bindings conj (->RuleExpr rule-name normalized-args)))))

(defn- extract-find-symbols [[typ pattern]]
  (case typ
    ::find-rel (mapcat extract-find-symbols pattern)
    :var       [pattern]
    :aggregate (mapcat extract-find-symbols (:fn-args pattern))))

(defmethod normalize ::find-rel [ctx [_ find-elems :as find-spec]]
  (let [ctx     (reduce normalize ctx find-elems)
        symbols (into [] (extract-find-symbols find-spec))]
    (update ctx :bindings conj (->Projection symbols nil))))

(defmethod normalize :var [ctx _] ctx)

(defmethod normalize :aggregate [ctx [_ {:keys [aggregation-fn fn-args]}]]
  (let [{:keys [inputs normalized-args]} (const->in fn-args)]
    (-> ctx
        (update :bindings into inputs)
        (update :bindings conj (->Aggregation aggregation-fn normalized-args nil)))))

(defn normalize-query
  ([ir] (normalize-query (->Context #{} ::and []) ir))
  ([ctx ir]
   (let [where (reduce normalize (->Context #{} ::and []) (:where ir))
         in    (->Context (into #{} (map #(->Input % nil)) (:in ir)) ::and [])
         find  (normalize (->Context #{} ::and []) (:find ir))]
     (->Context #{} ::and [where in find]))))

(comment
  (-> '[:find ?e ?n :where [?e :name ?n]] (parse-query) (normalize-query))

  (-> '[:find ?user (min ?age) :where [?user :age ?age]] (parse-query) (normalize-query))
  
  (-> '[:find ?e
        :in ?name
        :where [?e :name ?name]] (parse-query) (normalize-query))
  
  (-> '[:find ?e ?n ?a
        :where
        [?e :name ?n] [?e :age ?a]
        (or [(< ?a 10)]
            [(> ?a 18)])] (parse-query) (normalize-query)))

;; UNIFICATION

;; We process the set of bindings until all conflicts are
;; resolved. Clauses are in conflict iff they share one or more
;; symbols. ific (i.e. nested) context they share. Conflicting
;; bindings are resolved via unification. Context determines the
;; method of unification (conjunction / disjunction). Conflicts must
;; be resolved within the most specific (i.e. deeply nested) contexts
;; first, before continuing up the hierarchy (which means that
;; introducing clauses one-by-one doesn't fully cut it). If a symbol
;; is bound in two different contexts, the enclosing one determines
;; the method of unification.

;; @TODO join more than two bindings
(defrecord Join [bindings]
  IBinding
  (bound-symbols [this]
    (let [shared    (apply shared-symbols bindings)
          join-syms (into [] shared)]
      (reduce concat join-syms (map #(remove shared (bound-symbols %)) bindings))))
  (plan [this]
    (let [symbols (bound-symbols this)]
      {:Join [(plan (first (.-bindings this))) (plan (second (.-bindings this))) (into [] (apply shared-symbols bindings))]})))

;; @TODO antijoin more than two bindings
(defrecord Antijoin [bindings]
  IBinding
  (bound-symbols [this]
    (let [shared      (apply shared-symbols bindings)
          join-syms   (into [] shared)]
      (concat join-syms (remove shared (bound-symbols (first bindings))))))
  (plan [this]
    {:Antijoin [(plan (first bindings)) (plan (second bindings)) (into [] (apply shared-symbols bindings))]}))

(defrecord Union [bindings]
  IBinding
  (bound-symbols [this] (into [] (apply shared-symbols bindings)))
  (plan [this]
    (let [symbols (bound-symbols this)]
      (if (every? #(binds-all? % symbols) bindings)
        {:Union [symbols (mapv plan bindings)]}
        (if debug?
          {:Union [symbols (mapv plan bindings)]}
          (throw (ex-info "Bindings must be union compatible inside of an or-clause. Insert suitable projections."
                          {:unified (debug-plan this)})))))))

(defn- union
  "Unifies two conflicting relations by taking their union."
  [b1 b2]
  (case [(instance? Union b1) (instance? Union b2)]
    [false false] (->Union [b1 b2])
    [false true]  (update b2 :bindings conj b1)
    [true false]  (update b1 :bindings conj b2)
    [true true]   (update b1 :bindings into (.-bindings b2))))

;; Define conflicts and how to resolve them.

(derive Input ::binding)
(derive Relation ::binding)
(derive RuleExpr ::binding)
(derive Predicate ::binding)
(derive Aggregation ::binding)
(derive Projection ::binding)
(derive Join ::binding)
(derive Union ::binding)
(derive Antijoin ::binding)

(defmulti conflicting?
  "Returns true iff two bindings are in conflict."
  (fn [b1 b2] [(type b1) (type b2)]))

(defmethod conflicting? :default [b1 b2] (some? (seq (shared-symbols b1 b2))))
(defmethod conflicting? [Projection ::binding] [b1 b2] (binds-all? b2 (bound-symbols b1)))
(defmethod conflicting? [::binding Projection] [b1 b2] (conflicting? b2 b1))

(defmethod conflicting? [::binding Aggregation] [b1 b2]
  (conflicting? b2 b1))
(defmethod conflicting? [Aggregation ::binding] [b1 b2]
  (binds-all? b2 (bound-symbols b1)))
(defmethod conflicting? [Aggregation Projection] [b1 b2]
  (conflicting? b2 b1))
(defmethod conflicting? [Projection Aggregation] [b1 b2]
  (some? (seq (shared-symbols b1 b2))))

(defmethod conflicting? [::binding Predicate] [b1 b2]
  (conflicting? b2 b1))
(defmethod conflicting? [Predicate ::binding] [b1 b2]
  (binds-all? b2 (bound-symbols b1)))
(defmethod conflicting? [Input Predicate] [b1 b2]
  (conflicting? b2 b1))
(defmethod conflicting? [Predicate Input] [b1 b2]
  (some? (seq (shared-symbols b1 b2))))
(defmethod conflicting? [Predicate Predicate] [b1 b2]
  (binds-all? b2 (bound-symbols b1)))
(defmethod conflicting? [Predicate Projection] [b1 b2]
  (conflicting? b2 b1))
(defmethod conflicting? [Projection Predicate] [b1 b2]
  (some? (seq (shared-symbols b1 b2))))

(comment
  (conflicting? (->Relation nil nil '[?e ?a]) (->Relation nil nil '[?a]))
  (conflicting? (->Projection '[?e ?a] nil) (->Relation nil nil '[?a ?e]))
  (conflicting? (->Relation nil nil '[?a]) (->Projection '[?e ?a] nil))
  (conflicting? (->Projection '[?e ?a] nil) (->Aggregation nil '[?a] nil))
  (conflicting? (->Projection '[?key ?t1 ?t2] nil)
                (->Join [(->Relation nil nil '[?key ?op1 ?t1 ?op2 ?t2])
                         (->Relation nil nil '[?key ?op1 ?t1 ?op2 ?t2])]))
  )

(defmulti unify
  "Resolves two conflicting bindings."
  (fn [b1 b2 op] [(type b1) (type b2)]))

(defmethod unify :default [b1 b2 op]
  (cond
    (nil? b1) b2
    (nil? b2) b1
    :else
    (case [op (:negated? b1 false) (:negated? b2 false)]
      [::and false false] (->Join [b1 b2])
      [::and false true]  (->Antijoin [b1 b2])
      [::and true false]  (->Antijoin [b2 b1])
      [::or false false]  (union b1 b2)
      [::or false true]   (throw (ex-info "Unbound not" {:b1 b1 :b2 b2}))
      [::or true false]   (throw (ex-info "Unbound not" {:b1 b1 :b2 b2})))))

(defmethod unify [::binding Projection] [b1 b2 op] (unify b2 b1 op))
(defmethod unify [Projection ::binding] [projection binding op]
  (update projection :binding unify binding op))
(defmethod unify [Projection Projection] [proj1 proj2 op]
  (let [syms1 (set (bound-symbols proj1))
        syms2 (set (bound-symbols proj2))]
    (cond
      (= syms1 syms2) (update proj1 :binding unify (:binding proj2) op)
      (= op ::or)     (union proj1 proj2)
      :else           (throw (ex-info "Clashing projections." {:proj1 proj1 :proj2 proj2})))))

(defmethod unify [::binding Aggregation] [b1 b2 op] (unify b2 b1 op))
(defmethod unify [Aggregation ::binding] [aggregation binding op]
  (update aggregation :binding unify binding op))
(defmethod unify [Aggregation Projection] [aggregation projection op]
  (update aggregation :binding unify projection op))
(defmethod unify [Projection Aggregation] [projection aggregation op]
  (update projection :binding unify aggregation op))

(defmethod unify [Input Predicate] [b1 b2 op] (unify b2 b1 op))
(defmethod unify [Predicate Input] [pred inp op]
  (if-some [binding (.-binding pred)]
    (update pred :binding unify inp op)
    (update pred :symbols #(->> %
                                (into [] (remove (set (bound-symbols inp))))))))
(defmethod unify [::binding Predicate] [b1 b2 op] (unify b2 b1 op))
(defmethod unify [Predicate ::binding] [pred binding op]
  (update pred :binding unify binding op))
(defmethod unify [Predicate Projection] [b1 b2 op] (unify b2 b1 op))
(defmethod unify [Projection Predicate] [proj pred op]
  (update proj :binding unify pred op))
(defmethod unify [Predicate Predicate] [p1 p2 op]
  ;; @TODO compose predicates
  (case op
    ::and (update p1 :binding unify p2 op)
    ::or (union p1 p2)))

(defn- unify-with
  [op unified binding]
  (let [[conflicts free] (separate #(conflicting? binding %) unified)]
    (if (empty? conflicts)
      (conj unified binding)
      (let [resolved (reduce (fn [binding other]
                               (unify binding other op)) binding conflicts)]
        (unify-with op free resolved)))))

(defn unify-bindings
  ([op bindings] (unify-bindings op #{} bindings))
  ([op unified bindings]
   (if (empty? bindings)
     (do (trace-bindings "UNIFIED" unified)
         unified)
     (reduce (fn [unified binding]
               (trace-bindings (name op) [binding])
               (unify-with op unified binding)) unified bindings))))

(defn unify-context [^Context ctx]
  (if (empty? (.-children ctx))
    (update ctx :bindings (partial unify-bindings (.-op ctx)))
    (let [children (map unify-context (.-children ctx))]
      (-> ctx
          (assoc :children [])
          (update :bindings set/union (apply set/union (map :bindings children)))
          (unify-context)))))

;; PUTTING IT ALL TOGETHER

(defrecord Rule [name plan])

(defn- extract-binding
  "Extracts the final remaining binding from a context of unified
  bindings. Will throw if more than one binding is still present."
  [^Context ctx]
  (let [unified (.-bindings ctx)]
    (cond
      (empty? unified)            (throw (ex-info "No binding available." {:unified (debug-plan unified)}))
      (> (count unified) 1)       (throw (ex-info "More than one binding present." {:unified (mapv debug-plan unified)}))
      (:negated? (first unified)) (throw (ex-info "Unbound not." {:unified (debug-plan unified)}))
      :else                       (first unified))))

(defn compile-query [query]
  (let [ir  (parse-query query)
        ctx (normalize-query ir)]
    (-> ctx unify-context extract-binding plan)))

(comment
  (compile-query '[:find ?e ?n :where [?e :name ?n]])
  (compile-query '[:find ?n ?e :where [?e :name ?n]])
  (compile-query '[:find ?e :where [?e :name "Test"]])

  (compile-query '[:find ?e ?n ?a :where [?e :age ?a] [?e :name ?n]])

  (compile-query '[:find ?e ?a
                   :where
                   [?e :age ?a]
                   (or [?e :name "Mabel"] [?e :name "Dipper"])])

  (compile-query '[:find ?user (min ?age) :where [?user :age ?age]])

  (compile-query '[:find ?key ?t1 ?t2
                   :where
                   [?op1 :timestamp ?t1] [?op1 :key ?key]
                   [?op2 :timestamp ?t2] [?op2 :key ?key]
                   [(< ?t1 ?t2)]])

  (compile-query '[:find ?e :where (not [?e :name "Mabel"])])
  
  (compile-query '[:find ?e ?n ?a
                   :where
                   [?e :name ?n] [?e :age ?a]
                   (or [(< ?a 10)]
                       [(> ?a 18)])])
  )

(defn compile-rules [rules]
  (let [ir       (parse-rules rules)
        get-head #(get-in % [0 :head])
        by-head  (group-by get-head ir)

        ;; We perform a transformation here, wrapping body clauses
        ;; with (and) and rule definitions with (or). Note that :Union
        ;; with a single relation is equivalent to a :Project.
        get-clauses  #(get-in % [0 :clauses])
        wrap-and     (fn [clauses] [::and {:clauses clauses}])
        rewrite-rule (fn [rewritten head rules]
                       (let [wrapped-clauses (map (comp wrap-and get-clauses) rules)]
                         (if (= (count rules) 1)
                           (assoc rewritten head (get-clauses (first rules)))
                           (assoc rewritten head [[::or-join {:symbols (:vars head)
                                                              :clauses wrapped-clauses}]]))))
        rewritten    (reduce-kv rewrite-rule {} by-head)
        
        compile-rewritten (fn [compiled head where-clause]
                            (let [find-clause [::find-rel (mapv (fn [var] [:var var]) (:vars head))]
                                  where       (reduce normalize (->Context #{} ::and []) where-clause)
                                  find        (normalize (->Context #{} ::and []) find-clause)
                                  ctx         (->Context #{} ::and [where find])]
                              (assoc compiled head (unify-context ctx))))
        compiled          (reduce-kv compile-rewritten {} rewritten)

        rel->rule (fn [rules head ctx]
                    (let [rule-name (str (:name head))
                          plan      (->> ctx extract-binding plan)]
                      (conj rules {:name rule-name :plan plan})))
        rules     (reduce-kv rel->rule #{} compiled)]
    (into [] rules)))

(comment
  (compile-rules '[[(admin? ?user) [?user :admin? true]]])

  (compile-rules '[[(propagate ?x ?y) [?x :node ?y]]
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
    (compile-query q2))
  )

