(ns clj-3df.core
  (:refer-clojure :exclude [resolve])
  (:require
   [clojure.string :as str]
   [clojure.set :as set]
   #?(:clj [clojure.spec.alpha :as s]
      :cljs [cljs.spec.alpha :as s])
   [clj-3df.parser :as parser]))

(def ^{:arglists '([db query])} plan-query parser/plan-query)
(def ^{:arglists '([db rules])} plan-rules parser/plan-rules)

(defrecord Differential [schema attr->int int->attr next-tx impl registrations])

(defn create-db [schema]
  (let [attr->int (zipmap (keys schema) (iterate (partial + 100) 100))
        int->attr (set/map-invert attr->int)]
    (Differential. schema attr->int int->attr 0 nil {})))

