(ns web-after-tomorrow
  (:require
   [clj-3df.core :as df :use [exec!]]))



(def schema
  {:system/time {:db/valueType :Number}

   :read-access? {:db/valueType :Eid}
   
   :branch/subsidy {:db/valueType :Eid}
   :branch/account {:db/valueType :Eid}
   
   :loan/amount {:db/valueType :Number}
   :loan/from   {:db/valueType :Eid}
   :loan/to     {:db/valueType :Eid}
   :loan/payday {:db/valueType :Number}

   :person/name    {:db/valueType :String}
   :person/opt-in? {:db/valueType :Bool}})

(def db (df/create-db schema))



(do
  (def system 0)
  (def mabel 1)
  (def dipper 2)
  (def soos 3)
  (def loan-1 4) (def loan-2 5) (def loan-3 6) (def loan-4 7)
  (def branch-parent 8) (def branch-child 9)
  (def user 10))



(def initial-data
  [;; We start at day 1.
   {:db/id system :system/time 1}

   {:db/id branch-parent :branch/subsidy branch-child}

   {:db/id          mabel
    :person/name    "Mabel"
    :person/opt-in? true}

   {:db/id          dipper
    :person/name    "Dipper"
    :person/opt-in? true}

   ;; Soos has not opted-in yet.
   {:db/id       soos
    :person/name "Soos"}

   ;; All are part of the same subsidy.

   [:db/add branch-child :branch/account mabel]
   [:db/add branch-child :branch/account dipper]
   [:db/add branch-child :branch/account soos]

   ;; Initially we have a read permission
   ;; for the child branch

   [:db/add user :read-access? branch-child]

   ;; Mabel has issued two loans to Soos and Dipper,
   ;; both are due yesterday.
   
   {:db/id       loan-1
    :loan/from   mabel
    :loan/to     soos
    :loan/amount 50
    :loan/payday 0}

   {:db/id       loan-2
    :loan/from   mabel
    :loan/to     dipper
    :loan/amount 75
    :loan/payday 0}])



;; When is a loan considered due?

(def rules
  '[[(due? ?loan)
     [?loan :loan/payday ?payday]
     [?system :system/time ?time]
     [(<= ?payday ?time)]]])



(comment

  ;; Connect to the 3DF cluster.
  
  (do
    (def conn (df/create-debug-conn "ws://127.0.0.1:6262"))
    (exec! conn (df/create-db-inputs db)))



  ;; Transact initial data.

  (do
    (println "\n--- initial data ---")
    (exec! conn (df/transact db initial-data)))



  ;; Queries are named and made available globally.
  
  (exec! conn
    (df/register-query
     db "conjbank/opt-ins"
     '[:find ?person ?name
       :where
       [?person :person/opt-in? true]
       [?branch :branch/account ?person]
       [?person :person/name ?name]
       (read? ?branch)]
     
     '[[(read? ?branch)
        [?e :read-access? ?branch]]
       
       [(read? ?branch)
        [?super :branch/subsidy ?branch]
        (read? ?super)]]))


  
  (do
    (println "\n--- we lose access on the child...")
    (exec! conn
      (df/transact
       db
       [[:db/retract user :read-access? branch-child]])))


  
  (do
    (println "\n--- ...but gain access to the parent")
    (exec! conn
      (df/transact
       db
       [[:db/add user :read-access? branch-parent]])))


  
  ;; Queries can use both globally registered rules,
  ;; as well as locally defined ones.
  
  (exec! conn
    (df/register-query
     db "conjbank/debts"
     '[:find ?name (sum ?amount)
       :where
       (conjbank/opt-ins ?person ?name)
       [?loan :loan/from ?anyone]
       [?loan :loan/to ?person]
       [?loan :loan/amount ?amount]
       (due? ?loan)]
     rules))

  

  ;; Mabel's account manager is interested in the total
  ;; amount owed to her.

  (exec! conn
    (df/register-query
     db "session-1234/receivables"
     '[:find (sum ?amount)
       :where
       (conjbank/opt-ins ?debtor ?debtor-name)
       [?mabel :person/name "Mabel"]
       [?loan :loan/from ?mabel]
       [?loan :loan/to ?debtor]
       [?loan :loan/amount ?amount]
       (due? ?loan)]
     rules))



  ;; Soos's debts are not visible,
  ;; as he hasn't opted-in yet!

  (do
    (println "\n--- soos gives his consent ---")
    (exec! conn
      (df/transact
       db
       [[:db/add soos :person/opt-in? true]])))



  ;; Two other loans are due in a few days.

  (do
    (println "\n--- two additional loans at day 5")
    (exec! conn
      (df/transact
       db
       [{:db/id       loan-3
         :loan/from   mabel
         :loan/to     soos
         :loan/amount 10
         :loan/payday 5}

        {:db/id       loan-4
         :loan/from   dipper
         :loan/to     soos
         :loan/amount 5
         :loan/payday 5}])))
  
  
  
  ;; Some days pass...

  (do
    (println "\n--- advance system time ---")
    (exec! conn
      (df/transact
       db
       [[:db/retract 0 :system/time 1]
        [:db/add 0 :system/time 6]])))
  
  )

