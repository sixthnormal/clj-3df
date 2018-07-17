(ns clj-3df.integration-test
  (:require
   [clojure.test :refer [deftest is testing run-tests]]
   [clj-3df.core :as df :refer [exec! create-conn register-query transact]]
   [clj-3df.parser :as parser]))

(deftest test-min
  (let [db       (df/create-db {:age {:db/valueType :Number}})
        query    '[:find ?user (min ?age)
                   :where [?user :age ?age]]
        compiled (parser/compile-query db query)
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
        compiled (parser/compile-query db query)
        conn     (create-conn "ws://127.0.0.1:6262")]
    
    (exec! conn (register-query db "max" query))
    (Thread/sleep 1000)
    (exec! conn (transact db [{:db/id 1 :age 12}
                              {:db/id 2 :age 25}]))
    (Thread/sleep 1000)
    (exec! conn (transact db [[:db/add 3 :age 5]]))))

(deftest test-count
  (let [db       (df/create-db {:age {:db/valueType :Number}})
        query    '[:find (count ?user)
                   :where [?user :age ?age]]
        compiled (parser/compile-query db query)
        conn     (create-conn "ws://127.0.0.1:6262")]
    
    (exec! conn (register-query db "count" query))
    (Thread/sleep 1000)
    (exec! conn (transact db [{:db/id 1 :age 12}
                              {:db/id 2 :age 25}]))
    (Thread/sleep 1000)
    (exec! conn (transact db [[:db/add 3 :age 5]]))

    (Thread/sleep 1000)
    (exec! conn (transact db [[:db/retract 3 :age 5]]))))
