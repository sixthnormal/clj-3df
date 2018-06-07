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
   :edge   {:db/valueType :Eid}})

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
    
    (is (= {:Project [{:Join [{:HasAttr [2 100 1]}
                              {:HasAttr [0 100 1]} 1]} [0 1 2]]}
           (df/plan-query db query)))))

(deftest test-simple-join
  (let [query '[:find ?e ?n ?a
                :where [?e :name ?n] [?e :age ?a]]]
    (is (= {:Project [{:Join [{:HasAttr [0 200 2]}
                              {:HasAttr [0 100 1]} 0]} [0 1 2]]}
           (df/plan-query db query)))))

(deftest test-nested-join
  (let [query '[:find ?e1 ?n1 ?e2 ?n2
                :where [?e1 :name ?n1] [?e1 :friend ?e2] [?e2 :name ?n2]]]
    (is (= {:Project [{:Join [{:HasAttr [2 100 3]}
                              {:Join [{:HasAttr [0 300 2]}
                                      {:HasAttr [0 100 1]} 0]} 2]} [0 1 2 3]]}
           (df/plan-query db query)))))

(deftest test-simple-or
  (let [query '[:find ?e
                :where (or [?e :name "Dipper"]
                           [?e :age 12])]]
    (is (= {:Project [{:Union [{:Filter [0 200 {:Number 12}]}
                               {:Filter [0 100 {:String "Dipper"}]} [0]]} [0]]}
           (df/plan-query db query)))))

(deftest test-nested-or-and-filter
  (let [query '[:find ?e
                :where (or [?e :name "Dipper"]
                           (and [?e :name "Mabel"]
                                [?e :age 12]))]]
    (is (= {:Project
            [{:Union
              [{:Join [{:Filter [0 200 {:Number 12}]}
                       {:Filter [0 100 {:String "Mabel"}]} 0]}
               {:Filter [0 100 {:String "Dipper"}]} [0]]} [0]]}
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
    
    (is (= {:Project [{:Union [{:Join [{:HasAttr [2 400 1]}
                                       {:HasAttr [0 400 2]} 2]}
                               {:HasAttr [0 400 1]} [0 1]]} [0 1]]}
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
            [{:Union [{:Not {:Filter [0 100 {:String "Mabel"}]}}
                      {:Filter [0 100 {:String "Mabel"}]} [0]]} [0]]}
           (df/plan-query db query)))))

(deftest test-reachability
  (let [query '[:find ?x ?y
                :where
                (or-join [?x ?y]
                  [?x :edge ?y]
                  (and [?x :edge ?z]
                       (recur ?z ?y)))]]
    (is (= {:Project [{:Union [{:Join [{:Rule ["recur" [2 1]]}
                                       {:HasAttr [0 400 2]} 2]}
                               {:HasAttr [0 400 1]} [0 1]]} [0 1]]}
           (df/plan-query db query)))))

(deftest test-label-propagation
  (let [query '[:find ?x ?y
                :where
                (or-join [?x ?y]
                  [?x :node ?y]
                  (and [?z :edge ?y]
                       (recur ?x ?z)))]]
    (is (= {:Project
            [{:Union
              [{:Join [{:Rule ["recur" [0 2]]}
                       {:HasAttr [2 400 1]} 2]}
               {:HasAttr [0 nil 1]} [0 1]]} [0 1]]}
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
            [{:Union
              [{:Join
                [{:Filter [0 200 {:Number 12}]}
                 {:Filter [0 100 {:String "Dipper"}]} 0]}
               {:Filter [0 100 {:String "Mabel"}]} [0]]} [0]]}
           (df/plan-query db query)))))

(deftest test-simple-rule
  (let [rules '[[(premium? ?user) [?user :user/purchase-amount 1000]]]]
    (is (= '[[{:head {:name premium?, :vars [?user]},					
               :clauses [[:clj-3df.core/filter [?user :user/purchase-amount [:number 1000]]]]}]]
           (df/parse-rules rules)))))

(deftest test-recursive-rule
  (let [rules '[[(propagate ?x ?y) [?x :node ?y]]
                [(propagate ?x ?y) [?z :edge ?y] (propagate ?x ?z)]]]
    (is (= '[[{:head {:name propagate, :vars [?x ?y]},
					     :clauses [[:clj-3df.core/hasattr [?x :node ?y]]]}]
					   [{:head {:name propagate, :vars [?x ?y]},
					     :clauses
					     [[:clj-3df.core/hasattr [?z :edge ?y]]
					      [:clj-3df.core/rule-expr
					       {:rule-name propagate, :symbols [?x ?z]}]]}]]
           (df/parse-rules rules)))))
