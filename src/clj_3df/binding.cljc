(ns clj-3df.binding
  (:require
   [clj-3df.encode :as encode]))

(defn attribute [[e name v]]
  {:Attribute
   {:variables        [e v]
    :source_attribute name}})

(defn optional-attribute [[e name v default]]
  {:Attribute
   {:variables        [e v]
    :source_attribute name
    :default          (encode/encode-value default)}})

(defn constant [[symbol value]]
  {:Constant
   {:variable symbol
    :value    (encode/encode-value value)}})

(defn binary-predicate [[predicate x y]]
  {:BinaryPredicate
   {:variables [x y]
    :predicate (encode/encode-predicate predicate)}})

(comment

  (attribute '[?e :name ?n])
  (optional-attribute '[?e :admin? ?admin false])
  (constant '[?c 123])
  (binary-predicate '(<= ?v1 ?v2))
  
  )
