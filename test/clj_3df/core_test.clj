(ns clj-3df.core-test
  (:require
   [clojure.test :refer [deftest is testing run-tests]]
   [clj-3df.core :as df]
   [clj-3df.parser :as parser]))

;; CONFIGURATION

(def schema
  {:name    {:db/valueType :String}
   :age     {:db/valueType :Number}
   :friend  {:db/valueType :Eid}
   :edge    {:db/valueType :Eid}
   :admin?  {:db/valueType :Bool}
   :node    {:db/valueType :Eid}
   :extends {:db/valueType :Eid}})

(def db (df/create-db schema))

;; HELPER

(defn- plan-query
  "Helper to keep plan-only tests focused."
  [db query]
  (.-plan (parser/compile-query db query)))

;; TESTS

(deftest test-lookup
  (let [query '[:find ?n :where [876 :name ?n]]]
    (is (= {:Lookup [876 100 0]}
           (plan-query db query)))))

(deftest test-entity
  (let [query '[:find ?a ?v :where [429 ?a ?v]]]
    (is (= {:Entity [429 0 1]}
           (plan-query db query)))))

(deftest test-hasattr
  (let [query '[:find ?e ?v :where [?e :name ?v]]]
    (is (= {:HasAttr [0 100 1]}
           (plan-query db query)))))

(deftest test-filter
  (let [query '[:find ?e :where [?e :name "Dipper"]]]
    (is (= {:Filter [0 100 {:String "Dipper"}]}
           (plan-query db query)))))

(deftest test-find-clause
  (let [query '[:find ?e1 ?n ?e2
                :where [?e1 :name ?n] [?e2 :name ?n]]]

    (testing "error is thrown on unbound symbols in a find-clause"
      (is (thrown? Exception (plan-query db '[:find ?unbound :where [?bound :name "Dipper"]]))))
    
    (is (= {:Project
            [{:Join [{:HasAttr [2 100 1]} {:HasAttr [0 100 1]} 1]} [0 1 2]]}
           (plan-query db query)))))

(deftest test-simple-join
  (let [query '[:find ?e ?n ?a
                :where [?e :name ?n] [?e :age ?a]]]
    (is (= {:Project
            [{:Join [{:HasAttr [0 200 2]} {:HasAttr [0 100 1]} 0]} [0 1 2]]}
           (plan-query db query)))))

(deftest test-nested-join
  (let [query '[:find ?e1 ?n1 ?e2 ?n2
                :where [?e1 :name ?n1] [?e1 :friend ?e2] [?e2 :name ?n2]]]
    (is (= {:Project
            [{:Join
              [{:Join [{:HasAttr [2 100 3]} {:HasAttr [0 300 2]} 2]}
               {:HasAttr [0 100 1]}
               0]}
             [0 1 2 3]]}
           (plan-query db query)))))

(deftest test-simple-or
  (let [query '[:find ?e
                :where (or [?e :name "Dipper"]
                           [?e :age 12])]]
    (is (= {:Union [[0]
                    [{:Filter [0 200 {:Number 12}]}
                     {:Filter [0 100 {:String "Dipper"}]}]]}
           (plan-query db query)))))

(deftest test-multi-arity-join
  (let [query '[:find ?e
                :where (or [?e :name "Dipper"]
                           [?e :age 12]
                           [?e :admin? false])]]
    (is (= {:Union [[0]
                    [{:Filter [0 500 {:Bool false}]}
                     {:Filter [0 200 {:Number 12}]}
                     {:Filter [0 100 {:String "Dipper"}]}]]}
           (plan-query db query)))))

(deftest test-nested-or-and-filter
  (let [query '[:find ?e
                :where (or [?e :name "Dipper"]
                           (and [?e :name "Mabel"]
                                [?e :age 12]))]]
    (is (= {:Union
            [[0]
             [{:Join [{:Filter [0 200 {:Number 12}]}
                      {:Filter [0 100 {:String "Mabel"}]}
                      0]}
              {:Filter [0 100 {:String "Dipper"}]}]]}
           (plan-query db query)))))

(deftest test-or-join
  (let [query '[:find ?x ?y
                :where (or-join [?x ?y]
                         [?x :edge ?y]
                         (and [?x :edge ?z]
                              [?z :edge ?y]))]]

    (testing "plain or not allowed if symbols don't match everywhere"
      (is (thrown? Exception (plan-query db '[:find ?x ?y
                                              :where (or [?x :edge ?y]
                                                         (and [?x :edge ?z] [?z :edge ?y]))]))))
    
    (is (= {:Union [[0 1]
                    [{:Join [{:HasAttr [2 400 1]} {:HasAttr [0 400 2]} 2]}
                     {:HasAttr [0 400 1]}]]}
           (plan-query db query)))))

(deftest test-not
  (let [query '[:find ?e
                :where [?e :age 12] (not [?e :name "Mabel"])]]
    (is (= {:Antijoin
            [{:Filter [0 200 {:Number 12}]} {:Filter [0 100 {:String "Mabel"}]} [0]]}
           (plan-query db query)))))

(deftest test-fully-unbounded-not
  (let [query '[:find ?e
                :where (not [?e :name "Mabel"])]]
    ;; @TODO Decide what is the right semantics here. Disallow or negate?
    (is (thrown? Exception (plan-query db query)))))

(deftest test-tautology
  (let [query '[:find ?e
                :where (or [?e :name "Mabel"]
                           (not [?e :name "Mabel"]))]]
    (is (thrown? Exception (plan-query db query)))))

(deftest test-bounded-not
  (let [query '[:find ?e ?name
                :where
                [?e :name ?name]
                (or [?e :name "Mabel"]
                    (not [?e :name "Mabel"]))]]
    ;; @TODO
    #_(is (= {:Join
            [{:HasAttr [0 100 1]}
             {:Union [[0]
                      {:Filter [0 100 {:String "Mabel"}]}
                      {:Filter [0 ]}]}
             0]}
           (plan-query db query)))))

(deftest test-contradiction
  (let [query '[:find ?e
                :where (and [?e :name "Mabel"]
                            (not [?e :name "Mabel"]))]]
    (is (= {:Antijoin
            [{:Filter [0 100 {:String "Mabel"}]}
             {:Filter [0 100 {:String "Mabel"}]}
             [0]]}
           (plan-query db query)))))

(deftest test-reachability
  (let [query '[:find ?x ?y
                :where
                (or-join [?x ?y]
                  [?x :edge ?y]
                  (and [?x :edge ?z]
                       (recur ?z ?y)))]]
    (is (= {:Union [[0 1]
                    [{:Join [{:RuleExpr ["recur" [2 1]]} {:HasAttr [0 400 2]} 2]}
                     {:HasAttr [0 400 1]}]]}
           (plan-query db query)))))

(deftest test-label-propagation
  (let [query '[:find ?x ?y
                :where
                (or-join [?x ?y]
                  [?x :node ?y]
                  (and [?z :edge ?y]
                       (recur ?x ?z)))]]
    (is (= {:Union [[0 1]
                    [{:Join [{:RuleExpr ["recur" [0 2]]} {:HasAttr [2 400 1]} 2]}
                     {:HasAttr [0 600 1]}]]}
           (plan-query db query)))))

(deftest test-nested-and-or
  (let [query '[:find ?e
                :where (or [?e :name "Mabel"]
                           (and [?e :name "Dipper"]
                                [?e :age 12]))]]
    (is (= {:Union [[0]
                    [{:Join
                      [{:Filter [0 200 {:Number 12}]}
                       {:Filter [0 100 {:String "Dipper"}]} 0]}
                     {:Filter [0 100 {:String "Mabel"}]}]]}
           (plan-query db query)))))

(deftest test-simple-rule
  (let [rules '[[(admin? ?user) [?user :admin? true]]]]
    (is (= #{(parser/->Rule "admin?" {:Filter [0 500 {:Bool true}]})}
           (parser/compile-rules db rules)))))

(deftest test-recursive-rule
  (let [rules '[[(propagate ?x ?y) [?x :node ?y]]
                [(propagate ?x ?y) [?z :edge ?y] (propagate ?x ?z)]]]
    (is (= #{(parser/->Rule "propagate" {:Union
                                         [[0 1]
                                          [{:Join [{:RuleExpr ["propagate" [0 2]]} {:HasAttr [2 400 1]} 2]}
                                           {:HasAttr [0 600 1]}]]})}
           (parser/compile-rules db rules)))))

(deftest test-many-rule-bodies
  (let [rules '[;; @TODO check whether datomic allows this
                ;; [(subtype ?t1 ?t2) [?t2 :name "Object"]]
                [(subtype ?t1 ?t2) [?t1 :name ?any] [?t2 :name "Object"]]
                [(subtype ?t1 ?t2) [?t1 :name ?n] [?t2 :name ?n]]
                [(subtype ?t1 ?t2) [?t1 :extends ?t2]]
                [(subtype ?t1 ?t2) [?t1 :extends ?any] (subtype ?any ?t2)]]]
    (is (= #{(parser/->Rule "subtype" {:Union
                                       [[1 0]
                                        [{:Filter [0 100 {:String "Object"}]}
                                         {:Join [{:HasAttr [1 100 2]} {:HasAttr [0 100 2]} 2]}
                                         {:HasAttr [1 700 0]}
                                         {:Join
                                          [{:HasAttr [1 700 3]} {:RuleExpr ["subtype" [3 0]]} 3]}]]})}
           (parser/compile-rules db rules)))))

(deftest test-predicates
  (let [query    '[:find ?a1 ?a2
                   :where
                   [?user :age ?a1]
                   [?user :age ?a2]
                   [(< ?a1 ?a2)]]
        compiled (parser/compile-query db query)]
    (is (= {:Project
            [{:PredExpr
              ["LT" [0 1] {:Join [{:HasAttr [2 200 1]} {:HasAttr [2 200 0]} 2]}]}
             [0 1]]}
           (.-plan compiled)))))

(deftest test-inputs
  (let [query    '[:find ?user ?age
                   :in ?max-age
                   :where
                   [?user :age ?age]
                   [(< ?age ?max-age)]]
        compiled (parser/compile-query db query)]
    (is (= {:PredExpr ["LT" [1 [:input 0]] {:HasAttr [0 200 1]}]}
           (.-plan compiled)))
    
    (is (= {'?max-age [:input 0]} (.-in compiled)))))

(deftest test-lww
  (let [schema {:assign/time  {:db/valueType :Number}
                :assign/key   {:db/valueType :Number}
                :assign/value {:db/valueType :String}}
        db     (df/create-db schema)
        rules  '[[(older? ?t1 ?key)
                  [?op :assign/key ?key] [?op :assign/time ?t1]
                  [?op2 :assign/key ?key] [?op2 :assign/time ?t2]
                  [(< ?t1 ?t2)]]

                 [(lww ?key ?val)
                  [?op :assign/time ?t]
                  [?op :assign/key ?key]
                  [?op :assign/value ?val]
                  (not (older? ?t ?key))]]]
    (is (= #{(parser/->Rule "older?"
                            {:Project
                             [{:PredExpr
                               ["LT"
                                [0 3]
                                {:Join
                                 [{:Join [{:HasAttr [2 100 3]} {:HasAttr [2 200 1]} 2]}
                                  {:Join [{:HasAttr [4 100 0]} {:HasAttr [4 200 1]} 4]}
                                  1]}]}
                              [0 1]]})
             (parser/->Rule "lww"
                            {:Project
                             [{:Antijoin
                               [{:Join
                                 [{:Join [{:HasAttr [2 300 1]} {:HasAttr [2 200 0]} 2]}
                                  {:HasAttr [2 100 3]}
                                  2]}
                                {:RuleExpr ["older?" [3 0]]}
                                [3 0]]}
                              [0 1]]})}
           (parser/compile-rules db rules)))))

(deftest test-min
  (let [query    '[:find ?user (min ?age)
                   :where [?user :age ?age]]
        compiled (parser/compile-query db query)]
    (is (= {:Aggregate ["min" {:HasAttr [0 200 1]} [1]]}
           (.-plan compiled)))))
