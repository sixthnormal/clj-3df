(ns multitenant
  (:require
   [clj-3df.core :as df :use [exec!]]
   [clj-3df.attribute :as attribute]
   [tea-time.core :as tt]))

;; We are modeling devices that are reporting their current speed via
;; some sensors. The speed is supposed to match the settings for that
;; device. We are interested in reporting any devices that are
;; deviating from their settings.

(def schema
  {:settings/speed (merge
                    (attribute/of-type :Number)
                    (attribute/input-semantics :db.semantics.cardinality/one))
   :device/name    (merge
                    (attribute/of-type :String)
                    (attribute/input-semantics :db.semantics.cardinality/one))
   :device/speed   (merge
                    (attribute/of-type :Number)
                    (attribute/input-semantics :db.semantics.cardinality/one))
   :param/device   (merge
                    (attribute/of-type :Eid)
                    (attribute/input-semantics :db.semantics.cardinality/many))})

(def db (df/create-db schema))

(declare conn)

(def generator
  (Thread.
   (fn []
     (loop []
       (exec! conn
         (df/transact db [[:db/add 111 :device/speed (rand-int 200)]
                          [:db/add 222 :device/speed (rand-int 200)]
                          [:db/add 333 :device/speed (rand-int 200)]
                          [:db/add 444 :device/speed (rand-int 200)]
                          [:db/add 555 :device/speed (rand-int 200)]]))
       (Thread/sleep 10)
       (recur)))))

(comment

  (def conn
    (df/create-debug-conn! "ws://127.0.0.1:6262"))

  (exec! conn
    (concat
     (df/create-db-inputs db)
     (df/register-query
      db "current-speed"
      '[:find ?tenant ?device ?speed
        :where
        [?device :device/speed ?speed]
        [?tenant :param/device ?device]])
     [{:Interest {:name        "current-speed"
                  :tenant      0
                  :sink        {:TheVoid "/Users/niko/data/results/reuse/times.csv"}
                  :granularity 1}}]
     (df/transact db [[:db/add 0 :param/device 111]])))

  (do
    (.start generator)
    (tt/start!)
    ;; every second, create 10 new dataflows
    #_(tt/every!
     1 5
     (bound-fn []
       (doseq [i (range 10)]
         (let [name (name (gensym))]
           (exec! conn
             (concat
              (df/register-query
               db name
               '[:find ?tenant ?device ?speed
                 :where
                 [?device :device/speed ?speed]
                 [?tenant :param/device ?device]])
              [{:Interest {:name name
                           :sink {:TheVoid nil}}}]))))))
    ;; every second, add 10 more interested clients
    #_(tt/every!
     1 5
     (bound-fn []
       (doseq [i (range 10)]
         (exec! conn
           [{:Interest {:name "current-speed"
                        ;; :tenant      0
                        :sink {:TheVoid nil}}}]))))
    ;; every second, add 10 more tenants
    (tt/every!
     1 5
     (bound-fn []
       (doseq [i (range 10)]
         (exec! conn
           (concat
            ;; (df/transact db [[:db/add i :param/device (rand-nth [111 222 333 444 555])]])
            [{:Interest {:name   "current-speed"
                         :tenant 0
                         :sink   {:TheVoid nil}}}])))))
    (.isAlive generator))

  (do
    (.stop generator)
    (tt/stop!)
    (tt/reset-tasks!))
  )
