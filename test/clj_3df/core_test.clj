(ns clj-3df.core-test
  (:require
   [clojure.test :refer [deftest is testing run-tests]]
   [clj-3df.parser :as parser :refer [compile-query compile-rules]]))

;; TESTS

(deftest test-lookup
  (let [query '[:find ?n :where [876 :name ?n]]]
    (is (= '{:Lookup [876 :name ?n]}
           (compile-query query)))))

(deftest test-entity
  (let [query '[:find ?a ?v :where [429 ?a ?v]]]
    (is (= '{:Entity [429 ?a ?v]}
           (compile-query query)))))

(deftest test-hasattr
  (let [query '[:find ?e ?v :where [?e :name ?v]]]
    (is (= '{:HasAttr [?e :name ?v]}
           (compile-query query)))))

(deftest test-filter
  (let [query '[:find ?e :where [?e :name "Dipper"]]]
    (is (= '{:Filter [?e :name "Dipper"]}
           (compile-query query)))))

(deftest test-find-clause
  (let [query '[:find ?e1 ?n ?e2
                :where [?e1 :name ?n] [?e2 :name ?n]]]

    (testing "error is thrown on unbound symbols in a find-clause"
      (is (thrown? Exception (compile-query '[:find ?unbound :where [?bound :name "Dipper"]]))))
    
    (is (= '{:Project
             [{:Join [{:HasAttr [?e1 :name ?n]} {:HasAttr [?e2 :name ?n]} ?n]} [?e1 ?n ?e2]]}
           (compile-query query)))))

(deftest test-simple-join
  (let [query '[:find ?e ?n ?a
                :where [?e :name ?n] [?e :age ?a]]]
    (is (= '{:Join [{:HasAttr [?e :name ?n]} {:HasAttr [?e :age ?a]} ?e]}
           (compile-query query)))))

(deftest test-nested-join
  (let [query '[:find ?e1 ?n1 ?e2 ?n2
                :where [?e1 :name ?n1] [?e1 :friend ?e2] [?e2 :name ?n2]]]
    (is (= '{:Project
             [{:Join
               [{:HasAttr [?e2 :name ?n2]}
                {:Join [{:HasAttr [?e1 :name ?n1]} {:HasAttr [?e1 :friend ?e2]} ?e1]}
                ?e2]}
              [?e1 ?n1 ?e2 ?n2]]}
           (compile-query query)))))

(deftest test-simple-or
  (let [query '[:find ?e
                :where (or [?e :name "Dipper"]
                           [?e :age 12])]]
    (is (= '{:Union [[?e] [{:Filter [?e :name "Dipper"]} {:Filter [?e :age 12]}]]}
           (compile-query query)))))

(deftest test-multi-arity-join
  (let [query '[:find ?e
                :where (or [?e :name "Dipper"]
                           [?e :age 12]
                           [?e :admin? false])]]
    (is (= '{:Union [[?e]
                     [{:Filter [?e :name "Dipper"]}
                      {:Filter [?e :age 12]}
                      {:Filter [?e :admin? false]}]]}
           (compile-query query)))))

(deftest test-nested-or-and-filter
  (let [query '[:find ?e
                :where (or [?e :name "Dipper"]
                           (and [?e :name "Mabel"]
                                [?e :age 12]))]]
    (is (= '{:Union
             [[?e]
              [{:Filter [?e :name "Dipper"]}
               {:Join [{:Filter [?e :age 12]} {:Filter [?e :name "Mabel"]} ?e]}]]}
           (compile-query query)))))

(deftest test-or-join
  (let [query '[:find ?x ?y
                :where (or-join [?x ?y]
                         [?x :edge ?y]
                         (and [?x :edge ?z]
                              [?z :edge ?y]))]]

    (testing "plain or not allowed if symbols don't match everywhere"
      (is (thrown? Exception (compile-query '[:find ?x ?y
                                              :where (or [?x :edge ?y]
                                                         (and [?x :edge ?z] [?z :edge ?y]))]))))
    
    (is (= '{:Union [[?x ?y]
                     [{:HasAttr [?x :edge ?y]}
                      {:Join [{:HasAttr [?z :edge ?y]} {:HasAttr [?x :edge ?z]} ?z]}]]}
           (compile-query query)))))

(deftest test-not
  (let [query '[:find ?e
                :where [?e :age 12] (not [?e :name "Mabel"])]]
    (is (= '{:Antijoin
             [{:Filter [?e :age 12]} {:Filter [?e :name "Mabel"]} [?e]]}
           (compile-query query)))))

(deftest test-fully-unbounded-not
  (let [query '[:find ?e
                :where (not [?e :name "Mabel"])]]
    ;; @TODO Decide what is the right semantics here. Disallow or negate?
    (is (thrown? Exception (compile-query query)))))

(deftest test-tautology
  (let [query '[:find ?e
                :where (or [?e :name "Mabel"]
                           (not [?e :name "Mabel"]))]]
    (is (thrown? Exception (compile-query query)))))

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
           (compile-query query)))))

(deftest test-contradiction
  (let [query '[:find ?e
                :where (and [?e :name "Mabel"]
                            (not [?e :name "Mabel"]))]]
    (is (= '{:Antijoin
             [{:Filter [?e :name "Mabel"]}
              {:Filter [?e :name "Mabel"]}
              [?e]]}
           (compile-query query)))))

(deftest test-reachability
  (let [query '[:find ?x ?y
                :where
                (or-join [?x ?y]
                  [?x :edge ?y]
                  (and [?x :edge ?z]
                       (recur ?z ?y)))]]
    (is (= '{:Union [[?x ?y]
                     [{:HasAttr [?x :edge ?y]}
                      {:Join [{:HasAttr [?x :edge ?z]} {:RuleExpr ["recur" [?z ?y]]} ?z]}]]}
           (compile-query query)))))

(deftest test-label-propagation
  (let [query '[:find ?x ?y
                :where
                (or-join [?x ?y]
                  [?x :node ?y]
                  (and [?z :edge ?y]
                       (recur ?x ?z)))]]
    (is (= '{:Union [[?x ?y]
                     [{:HasAttr [?x :node ?y]}
                      {:Join [{:HasAttr [?z :edge ?y]} {:RuleExpr ["recur" [?x ?z]]} ?z]}]]}
           (compile-query query)))))

(deftest test-nested-and-or
  (let [query '[:find ?e
                :where (or [?e :name "Mabel"]
                           (and [?e :name "Dipper"]
                                [?e :age 12]))]]
    (is (= '{:Union [[?e]
                     [{:Filter [?e :name "Mabel"]}
                      {:Join [{:Filter [?e :name "Dipper"]} {:Filter [?e :age 12]} ?e]}]]}
           (compile-query query)))))

(deftest test-simple-rule
  (let [rules '[[(admin? ?user) [?user :admin? true]]]]
    (is (= #{{:name "admin?" :plan '{:Filter [?user :admin? true]}}}
           (compile-rules rules)))))

(deftest test-recursive-rule
  (let [rules '[[(propagate ?x ?y) [?x :node ?y]]
                [(propagate ?x ?y) [?z :edge ?y] (propagate ?x ?z)]]]
    (is (= #{{:name "propagate"
              :plan '{:Union
                      [[?x ?y]
                       [{:HasAttr [?x :node ?y]}
                        {:Join [{:HasAttr [?z :edge ?y]}
                                {:RuleExpr ["propagate" [?x ?z]]} ?z]}]]}}}
           (compile-rules rules)))))

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
           (compile-rules rules)))))

(deftest test-predicates
  (let [query '[:find ?a1 ?a2
                :where
                [?user :age ?a1]
                [?user :age ?a2]
                [(< ?a1 ?a2)]]]
    (is (= '{:Project
             [{:PredExpr
               ["LT"
                [?a1 ?a2]
                {:Join [{:HasAttr [?user :age ?a2]} {:HasAttr [?user :age ?a1]} ?user]}]}
              [?a1 ?a2]]}
           (compile-query query)))))

(deftest test-inputs
  (let [query '[:find ?user ?age
                :in ?max-age
                :where
                [?user :age ?age]
                [(< ?age ?max-age)]]]
    (is (= '{:PredExpr ["LT" [?age ?max-age] {:HasAttr [?user :age ?age]}]}
           (compile-query query)))))

(deftest test-lww
  (let [rules '[[(older? ?t1 ?key)
                 [?op :assign/key ?key] [?op :assign/time ?t1]
                 [?op2 :assign/key ?key] [?op2 :assign/time ?t2]
                 [(< ?t1 ?t2)]]

                [(lww ?key ?val)
                 [?op :assign/time ?t]
                 [?op :assign/key ?key]
                 [?op :assign/value ?val]
                 (not (older? ?t ?key))]]]
    (is (= #{(parser/->Rule "older?"
                            '{:Project
                              [{:PredExpr
                                ["LT"
                                 [?t1 ?t2]
                                 {:Join
                                  [{:HasAttr [?op :assign/time ?t1]}
                                   {:Join
                                    [{:Join
                                      [{:HasAttr [?op2 :assign/time ?t2]}
                                       {:HasAttr [?op2 :assign/key ?key]}
                                       ?op2]}
                                     {:HasAttr [?op :assign/key ?key]}
                                     ?key]}
                                   ?op]}]}
                               [?t1 ?key]]})
             (parser/->Rule "lww"
                            '{:Project
                              [{:Antijoin
                                [{:Join
                                  [{:Join
                                    [{:HasAttr [?op :assign/value ?val]}
                                     {:HasAttr [?op :assign/time ?t]}
                                     ?op]}
                                   {:HasAttr [?op :assign/key ?key]}
                                   ?op]}
                                 {:RuleExpr ["older?" [?t ?key]]}
                                 (?t ?key ?op ?val)]}
                               [?key ?val]]})}
           (compile-rules rules)))))

(deftest test-min
  (let [query '[:find ?user (min ?age)
                :where [?user :age ?age]]]
    (is (= '{:Aggregate ["MIN" {:HasAttr [?user :age ?age]} [?age]]}
           (compile-query query)))))
