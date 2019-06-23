(ns clj-3df.schema
  (:require
   #?(:clj  [clojure.spec.alpha :as s]
      :cljs [cljs.spec.alpha :as s])))

(s/def :db/valueType
  '#{:String :Number :Bool
     :Eid
     :Aid
     :Instant
     :Uuid
     :Real})

(s/def :db/querySupport
  '#{:Basic :Delta :AdaptiveWCO})

(s/def :db/indexDirection
  '#{:Forward :Both})

(s/def :db/inputSemantics
  '#{:Raw :CardinalityOne :CardinalityMany})

(s/def :db/traceSlack
  (s/or
   :txid (s/keys :req-un [::TxId])
   :real (s/keys :req-un [::Real])
   :bi   (s/cat :time/duration :time/logical)))

(s/def ::attribute
  (s/keys :req [:db/valueType
                :db/inputSemantics
                :db/indexDirection
                :db/querySupport]
          :opt [:db/traceSlack]))

(comment
  
  (s/explain-data ::attribute {:db/valueType :String})

  (s/valid? ::attribute
            {:db/valueType      :String,
             :db/querySupport   :AdaptiveWCO,
             :db/indexDirection :Both,
             :db/inputSemantics :CardinalityMany,
             :db/traceSlack     {:TxId 1}})

  )
