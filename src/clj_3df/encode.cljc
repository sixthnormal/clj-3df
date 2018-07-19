(ns clj-3df.encode
  "Utilties for encoding query plans to something that the backend can
  understand. In particular, attributes and symbols will be encoded as
  integers.")

(def encode-symbol (memoize (fn [sym] (clojure.lang.RT/nextID))))

(defn- encode-plan [attr->int plan]
  (cond
    (map? plan)     (let [[tag args] (first plan)]
                      {tag (mapv (partial encode-plan attr->int) args)})
    (symbol? plan)  (encode-symbol plan)
    (keyword? plan) (attr->int plan)
    (vector? plan)  (mapv (partial encode-plan attr->int) plan)
    (string? plan)  {:String plan}
    (number? plan)  {:Number plan}
    (boolean? plan) {:Bool plan}))

(comment
  (encode-plan {} '?name)
  (encode-plan {:name 100} '{:HasAttr [?e :name ?n]})
  (encode-plan {:name 200} '{:Join [{:HasAttr [?e1 :name ?n]} {:HasAttr [?e2 :name ?n]} ?n]})
  (encode-plan {:name 300} '{:Join [{:HasAttr [?e1 :name ?n]} {:HasAttr [?e2 :name ?n]} ?n]})
  (encode-plan {:name 400} '{:Project
                             [{:Join [{:HasAttr [?e1 :name ?n]} {:HasAttr [?e2 :name ?n]} ?n]} [?e1 ?n ?e2]]})
  )
