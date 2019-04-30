(ns clj-3df.encode
  "Utilties for encoding query plans to something that the backend can
  understand. In particular, attributes and symbols will be encoded as
  integers."
  (:require [clojure.string :as str]))

(def nextID (atom 0))

(def encode-symbol (memoize (fn [sym] #?(:clj  (clojure.lang.RT/nextID)
                                         :cljs (swap! nextID inc)))))

(defn decode-value [v]
  (cond
    (contains? v :String) (:String v)
    (contains? v :Number) (:Number v)
    (contains? v :Aid)    (keyword (:Aid v))
    (contains? v :Bool)   (:String v)
    (contains? v :Eid)    (:Eid v)
    :else                 (throw (ex-info "Unknown value type" v))))

(defn encode-value [v]
  (cond
    (string? v)  {:String v}
    (number? v)  {:Number v}
    (keyword? v) {:Aid (subs (str v) 1)}
    (boolean? v) {:Bool v}))

(defn encode-keyword [kw]
  (subs (str kw) 1))

(def encode-predicate
  {'<    "LT"
   '<=   "LTE"
   '>    "GT"
   '>=   "GTE"
   '=    "EQ"
   'not= "NEQ"})

(def encode-fn (comp str/upper-case name))

(def encode-semantics
  {:db.semantics/raw              "Raw"
   :db.semantics.cardinality/one  "CardinalityOne"
   :db.semantics.cardinality/many "CardinalityMany"})

(defn encode-plan [plan]
  (cond
    (symbol? plan)      (encode-symbol plan)
    (keyword? plan)     (encode-keyword plan)
    (sequential? plan)  (mapv encode-plan plan)
    (associative? plan) (reduce-kv (fn [m k v] (assoc m k (encode-plan v))) {} plan)
    ;; (nil? plan)         (throw (ex-info "Plan contain's nils."
    ;;                                     {:causes #{:contains-nil}}))
    :else               plan))

(defn encode-rule [rule]
  (let [{:keys [name plan]} rule]
    {:name name
     :plan (encode-plan plan)}))

(defn encode-rules [rules]
  (mapv encode-rule rules))

(comment

  (encode-keyword :name)
  (encode-keyword :person/name)
  
  (encode-plan [])
  (encode-plan '?name)
  (encode-plan '{:MatchA [?e :name ?n]})
  (encode-plan '{:Join [?n {:MatchA [?e1 :name ?n]} {:MatchA [?e2 :name ?n]}]})
  (encode-plan '{:Join [?n {:MatchA [?e1 :name ?n]} {:MatchA [?e2 :name ?n]}]})
  (encode-plan '{:Project
                 [[?e1 ?n ?e2] {:Join [?n {:MatchA [?e1 :name ?n]} {:MatchA [?e2 :name ?n]}]}]})
  )
