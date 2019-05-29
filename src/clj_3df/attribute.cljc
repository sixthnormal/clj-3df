(ns clj-3df.attribute
  "Functions to combine the various attribute semantics into
  configurations that can be understood by the server."
  (:require
   [clj-3df.time :as time]
   [clj-3df.encode :as encode]))

(defn of-type
  "Specifies the value type of an attribute."
  [value-type]
  {:db/valueType    value-type
   :query_support   "AdaptiveWCO"
   :index_direction "Both"})

(defn input-semantics
  [semantics]
  {:input_semantics (encode/encode-semantics semantics)})

(defn tx-time
  "Specifies that an attribute will live in some transaction time domain
  and compact up to the computation frontier minus the specified
  slack."
  ([] (tx-time (time/tx-id 1)))
  ([slack] {:trace_slack slack}))

(defn real-time
  "Specifies that an attribute will live in some real-time domain and
  compact up to the computation frontier minus the specified slack."
  ([] (real-time (time/real-time 1 0)))
  ([slack] {:trace_slack slack}))

(defn uncompacted
  "Specifies that an attribute will live in an arbitrary time domain and
  never compact its trace."
  []
  {:trace_slack nil})

(comment

  ;; Creating a configuration is equivalent to merging together the
  ;; various semantics.

  (merge
   (of-type :String)
   (input-semantics :db.semantics.cardinality/one)
   (tx-time))

  (merge
   (input-semantics :db.semantics.cardinality/many)
   (real-time {:secs 10 :nanos 0}))

  (merge
   (input-semantics :db.semantics/raw)
   (uncompacted))

  )
