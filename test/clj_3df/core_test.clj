(ns clj-3df.core-test
  (:require
   [clojure.test :refer [deftest is testing run-tests]]
   [clj-3df.core :as df]))

;; CONFIGURATION

(def debug-test? true)

(def schema
  {:name   {:db/valueType :String}
   :age    {:db/valueType :Number}
   :friend {:db/valueType :Eid}
   :edge   {:db/valueType :Eid}
   :admin? {:db/valueType :Bool}})

(def db (df/create-db schema))

;; TESTS

(deftest test-lookup
  (let [query '[:find ?n :where [876 :name ?n]]]
    (is (= {:Project [{:Lookup [876 100 0]} [0]]}
           (df/plan-query db query)))))

(deftest test-entity
  (let [query '[:find ?a ?v :where [429 ?a ?v]]]
    (is (= {:Project [{:Entity [429 0 1]} [0 1]]}
           (df/plan-query db query)))))

(deftest test-hasattr
  (let [query '[:find ?e ?v :where [?e :name ?v]]]
    (is (= {:Project [{:HasAttr [0 100 1]} [0 1]]}
           (df/plan-query db query)))))

(deftest test-filter
  (let [query '[:find ?e :where [?e :name "Dipper"]]]
    (is (= {:Project [{:Filter [0 100 {:String "Dipper"}]} [0]]}
           (df/plan-query db query)))))

(deftest test-find-clause
  (let [query '[:find ?e1 ?n ?e2
                :where [?e1 :name ?n] [?e2 :name ?n]]]

    (testing "error is thrown on unbound symbols in a find-clause"
      (is (thrown? Exception (df/plan-query db '[:find ?unbound :where [?bound :name "Dipper"]]))))
    
    (is (= {:Project [{:Join [{:HasAttr [0 100 1]}
                              {:HasAttr [2 100 1]} 1]} [0 1 2]]}
           (df/plan-query db query)))))

(deftest test-simple-join
  (let [query '[:find ?e ?n ?a
                :where [?e :name ?n] [?e :age ?a]]]
    (is (= {:Project [{:Join [{:HasAttr [0 100 1]}
                              {:HasAttr [0 200 2]} 0]} [0 1 2]]}
           (df/plan-query db query)))))

(deftest test-nested-join
  (let [query '[:find ?e1 ?n1 ?e2 ?n2
                :where [?e1 :name ?n1] [?e1 :friend ?e2] [?e2 :name ?n2]]]
    (is (= {:Project [{:Join [{:Join [{:HasAttr [0 100 1]}
                                      {:HasAttr [0 300 2]} 0]}
                              {:HasAttr [2 100 3]} 2]} [0 1 2 3]]}
           (df/plan-query db query)))))

(deftest test-simple-or
  (let [query '[:find ?e
                :where (or [?e :name "Dipper"]
                           [?e :age 12])]]
    (is (= {:Project [{:Union [[0]
                               [{:Filter [0 100 {:String "Dipper"}]}
                                {:Filter [0 200 {:Number 12}]}]]} [0]]}
           (df/plan-query db query)))))


(deftest test-multi-arity-join
  (let [query '[:find ?e
                :where (or [?e :name "Dipper"]
                           [?e :age 12]
                           [?e :admin? false])]]
    (is (= {:Project [{:Union [[0]
                               [{:Filter [0 100 {:String "Dipper"}]}
                                {:Filter [0 200 {:Number 12}]}
                                {:Filter [0 500 {:Bool false}]}]]} [0]]}
           (df/plan-query db query)))))

(deftest test-nested-or-and-filter
  (let [query '[:find ?e
                :where (or [?e :name "Dipper"]
                           (and [?e :name "Mabel"]
                                [?e :age 12]))]]
    (is (= {:Project
            [{:Union
              [[0]
               [{:Filter [0 100 {:String "Dipper"}]}
                {:Join [{:Filter [0 100 {:String "Mabel"}]}
                        {:Filter [0 200 {:Number 12}]} 0]}]]} [0]]}
           (df/plan-query db query)))))

(deftest test-or-join
  (let [query '[:find ?x ?y
                :where (or-join [?x ?y]
                         [?x :edge ?y]
                         (and [?x :edge ?z]
                              [?z :edge ?y]))]]

    (testing "plain or not allowed if symbols don't match everywhere"
      (is (thrown? Exception (df/plan-query db '[:find ?x ?y
                                                 :where (or [?x :edge ?y]
                                                            (and [?x :edge ?z] [?z :edge ?y]))]))))
    
    (is (= {:Project [{:Union [[0 1]
                               [{:HasAttr [0 400 1]}
                                {:Join [{:HasAttr [0 400 2]}
                                        {:HasAttr [2 400 1]} 2]}]]} [0 1]]}
           (df/plan-query db query)))))

(deftest test-not
  (let [query '[:find ?e :where (not [?e :name "Mabel"])]]
    (is (= {:Project [{:Not {:Filter [0 100 {:String "Mabel"}]}} [0]]}
           (df/plan-query db query)))))

(deftest test-tricky-not
  (let [query '[:find ?e
                :where (or [?e :name "Mabel"]
                           (not [?e :name "Mabel"]))]]
    (is (= {:Project
            [{:Union [[0]
                      [{:Filter [0 100 {:String "Mabel"}]}
                       {:Not {:Filter [0 100 {:String "Mabel"}]}}]]} [0]]}
           (df/plan-query db query)))))

(deftest test-reachability
  (let [query '[:find ?x ?y
                :where
                (or-join [?x ?y]
                  [?x :edge ?y]
                  (and [?x :edge ?z]
                       (recur ?z ?y)))]]
    (is (= {:Project [{:Union [[0 1]
                               [{:HasAttr [0 400 1]}
                                {:Join [{:HasAttr [0 400 2]}
                                        {:RuleExpr ["recur" [2 1]]} 2]}]]} [0 1]]}
           (df/plan-query db query)))))

(deftest test-label-propagation
  (let [query '[:find ?x ?y
                :where
                (or-join [?x ?y]
                  [?x :node ?y]
                  (and [?z :edge ?y]
                       (recur ?x ?z)))]]
    (is (= {:Project
            [{:Union [[0 1]
                      [{:HasAttr [0 nil 1]}
                       {:Join [{:HasAttr [2 400 1]}
                               {:RuleExpr ["recur" [0 2]]} 2]}]]} [0 1]]}
           (df/plan-query db query)))))

(deftest test-de-morgans
  (is (= (df/plan-query db '[:find ?e :where (or (not [?e :name "Dipper"]) (not [?e :age 12]))])
         (df/plan-query db '[:find ?e :where (not (and [?e :name "Dipper"] [?e :age 12]))])))

  (is (= (df/plan-query db '[:find ?e :where (and (not [?e :name "Dipper"]) (not [?e :name "Mabel"]))])
         (df/plan-query db '[:find ?e :where (not (or [?e :name "Dipper"] [?e :name "Mabel"]))]))))

(deftest test-nested-and-or
  (let [query '[:find ?e
                :where (or [?e :name "Mabel"]
                           (and [?e :name "Dipper"]
                                [?e :age 12]))]]
    (is (= {:Project
            [{:Union [[0]
                      [{:Filter [0 100 {:String "Mabel"}]}
                       {:Join
                        [{:Filter [0 100 {:String "Dipper"}]}
                         {:Filter [0 200 {:Number 12}]} 0]}]]} [0]]}
           (df/plan-query db query)))))

(deftest test-simple-rule
  (let [rules '[[(admin? ?user) [?user :admin? true]]]]
    (is (= #{(df/->Rule "admin?" {:Project [{:Filter [0 500 {:Bool true}]} [0]]})}
           (df/plan-rules db rules)))))

(deftest test-recursive-rule
  (let [rules '[[(propagate ?x ?y) [?x :node ?y]]
                [(propagate ?x ?y) [?z :edge ?y] (propagate ?x ?z)]]]
    (is (= #{(df/->Rule "propagate" {:Union
                                     [[0 1]
                                      [{:HasAttr [0 nil 1]}
                                       {:Join [{:HasAttr [2 400 1]}
                                               {:RuleExpr ["propagate" [0 2]]} 2]}]]})}
           (df/plan-rules db rules)))))

(deftest test-many-rule-bodies
  (let [rules '[[(subtype ?t1 ?t2) [?t2 :name "Object"]]
                [(subtype ?t1 ?t2) [?t1 :name ?n] [?t2 :name ?n]]
                [(subtype ?t1 ?t2) [?t1 :extends ?t2]]
                [(subtype ?t1 ?t2) [?t1 :extends ?any] (subtype ?any ?t2)]]]
    (is (= #{(df/->Rule "subtype" {:Union
                                   [[1 0]
                                    [{:Filter [0 100 {:String "Object"}]}
                                     {:Join [{:HasAttr [1 100 2]} {:HasAttr [0 100 2]} 2]}
                                     {:HasAttr [1 nil 0]}
                                     {:Join
                                      [{:HasAttr [1 nil 3]} {:RuleExpr ["subtype" [3 0]]} 3]}]]})}
           (df/plan-rules db rules)))))
