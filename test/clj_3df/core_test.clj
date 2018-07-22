(ns clj-3df.core-test
  (:require
   [clojure.test :refer [deftest is testing run-tests]]
   [manifold.stream :as stream]
   [manifold.bus :as bus]
   [clj-3df.core :as df :refer [exec! create-conn register-query transact]]))

(defn- debug-conn []
  (let [conn (create-conn "ws://127.0.0.1:6262")]
    (stream/consume #(println %) (bus/subscribe (:out conn) :out))))

(deftest test-basics
  (let [schema {:name   {:db/valueType :String}
                :age    {:db/valueType :Number}
                :friend {:db/valueType :Eid}
                :admin? {:db/valueType :Bool}}
        db     (df/create-db schema)
        conn   (debug-conn)]

    (testing "Simple disjunction"
      (exec! conn
        (register-query db "simple-or" '[:find ?e :where (or [?e :name "Mabel"] [?e :name "Dipper"])])
        (transact db [[:db/add 1 :name "Dipper"] [:db/add 1 :age 26]])
        (expect-> out (is (= out '[[[{"Eid" 1}] 1]])))
        (transact db [{:db/id 2 :name "Mabel" :age 26}])
        (expect-> out (is (= out '[[[{"Eid" 2}] 1]])))
        (transact db [[:db/retract 2 :name "Mabel"]])
        (expect-> out (is (= out '[[[{"Eid" 2}] -1]])))))

    (testing "Simple negation"
      (exec! conn
        (register-query db "simple-not" '[:find ?e :where [?e :name "Mabel"] (not [?e :age 25])])
        (transact db [{:db/id 1 :name "Mabel" :age 25}])
        (transact db [{:db/id 2 :name "Mabel" :age 42}])
        (expect-> out (is (= out '[[[{"Eid" 2}] 1]])))))))

(deftest test-min
  (let [db       (df/create-db {:age {:db/valueType :Number}})
        query    '[:find ?user (min ?age)
                   :where [?user :age ?age]]
        conn     (create-conn "ws://127.0.0.1:6262")]
    
    (exec! conn (register-query db "min" query))
    (Thread/sleep 1000)
    (exec! conn (transact db [{:db/id 1 :age 12}
                              {:db/id 2 :age 25}]))
    (Thread/sleep 1000)
    (exec! conn (transact db [[:db/add 3 :age 5]]))))

(deftest test-max
  (let [db       (df/create-db {:age {:db/valueType :Number}})
        query    '[:find ?user (max ?age)
                   :where [?user :age ?age]]
        conn     (create-conn "ws://127.0.0.1:6262")]
    
    (exec! conn (register-query db "max" query))
    (Thread/sleep 1000)
    (exec! conn (transact db [{:db/id 1 :age 12}
                              {:db/id 2 :age 25}]))
    (Thread/sleep 1000)
    (exec! conn (transact db [[:db/add 3 :age 5]]))))

(deftest test-count
  (let [db    (df/create-db {:age {:db/valueType :Number}})
        query '[:find (count ?user)
                :where [?user :age ?age]]
        conn  (create-conn "ws://127.0.0.1:6262")]
    
    (exec! conn (register-query db "count" query))
    (Thread/sleep 1000)
    (exec! conn (transact db [{:db/id 1 :age 12}
                              {:db/id 2 :age 25}]))
    (Thread/sleep 1000)
    (exec! conn (transact db [[:db/add 3 :age 5]]))

    (Thread/sleep 1000)
    (exec! conn (transact db [[:db/retract 3 :age 5]]))))
