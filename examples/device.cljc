(ns device
  (:require
   [clj-3df.core :as df :use [exec!]]))

;; We are modeling devices that are reporting their current speed via
;; some sensors. The speed is supposed to match the settings for that
;; device. We are interested in reporting any devices that are
;; deviating from their settings.

(def schema
  {:settings/speed {:db/valueType :Number}
   :device/speed   {:db/valueType :Number}})

(def db (df/create-db schema))

(comment

  (do
    (def conn (df/create-debug-conn "ws://127.0.0.1:6262"))
    (exec! conn (df/create-db-inputs db)))

  (exec! conn
    (df/transact db [[:db/add 111 :settings/speed 100]]))

  (exec! conn
    (df/query
     db "current-speed"
     '[:find ?device ?speed 
       :where [?device :device/speed ?speed]]))

  (exec! conn
    (df/query
     db "current-settings"
     '[:find ?device ?speed 
       :where [?device :settings/speed ?speed]]))

  (exec! conn
    (df/query
     db "deviating"
     '[:find ?device ?deviation
       :where
       [?device :settings/speed ?target]
       [?device :device/speed ?speed]
       [(< ?speed ?target)]
       [(subtract ?target ?speed) ?deviation]]))

  (exec! conn
    (df/transact db [[:db/add 111 :device/speed 50]]))

  (exec! conn
    (df/transact db [[:db/retract 111 :device/speed 50]
                     [:db/add 111 :device/speed 75]]))

  )
