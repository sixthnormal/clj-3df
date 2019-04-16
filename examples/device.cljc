(ns device
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

   :param/device (merge
                  (attribute/of-type :Eid)
                  (attribute/input-semantics :db.semantics.cardinality/many))})

(def db (df/create-db schema))

(declare conn)

(def generator
  (Thread.
   (fn []
     (loop []
       (prn "looping")
       (exec! conn
         (df/transact db [[:db/add 111 :device/speed (rand-int 200)]
                          [:db/add 222 :device/speed (rand-int 200)]]))
       (Thread/sleep 1000)
       (recur)))))

(comment

  (def conn
    (df/create-debug-conn! "ws://127.0.0.1:6262"))

  (exec! conn
    (df/create-db-inputs db)
    #_(df/transact db [[:db/add 111 :device/name "dev0"]
                     [:db/add 111 :settings/speed 100]
                     [:db/add 222 :device/name "dev1"]
                     [:db/add 222 :settings/speed 130]]))

  (.start generator)
  (.stop generator)
  (.isAlive generator)

  (tt/start!)
  (tt/stop!)

  (tt/every!
   5 5
   (bound-fn []
     (exec! conn
       [{:Interest {:name "current-speed"
                    ;; :tenant      0
                    :sink :TheVoid}}])))
  
  (exec! conn
    (df/transact db [[:db/add 111 :device/speed 8]
                     [:db/add 222 :device/speed 10]]))

  (exec! conn
    (df/transact db [[:db/add 0 :param/device 333]]))
  
  (exec! conn
    (concat
     (df/register-query
      db "current-speed"
      '[:find ?tenant ?device ?speed
        :where
        [?device :device/speed ?speed]
        [?tenant :param/device ?device]])
     [{:Interest {:name        "current-speed"
                  ;; :tenant      0
                  :sink        {:TheVoid "/Users/niko/data/results/times.csv"}
                  :granularity 1}}]))

  (exec! conn
    (df/query
     db "current-settings"
     '[:find ?device ?speed 
       :where [?device :settings/speed ?speed]]))

  (exec! conn
    (df/query
     db "devicemgr/deviating"
     '[:find ?device
       :where
       [?device :settings/speed ?target]
       [?device :device/speed ?speed]
       [(< ?speed ?target)]]))

  (exec! conn
    (df/query
     db "devicemgr/alerts"
     '[:find ?device ?name ?deviation
       :where
       (devicemgr/deviating ?device)
       [?device :device/name ?name]
       [?device :settings/speed ?target]
       [?device :device/speed ?speed]
       [(subtract ?target ?speed) ?deviation]]))

  (exec! conn
    (df/transact db [[:db/add 111 :device/speed 87]]))

  (exec! conn
    (df/transact db [[:db/add 222 :device/speed 120]]))

  (exec! conn
    (df/uninterest "current-settings"))

  )
