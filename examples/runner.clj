(ns runner
  (:require
   [clojure.edn :as edn]
   [clj-3df.core :as df]))

(defn -main [& args]
  (when-let [filename (first args)]
    (let [config (-> filename slurp edn/read-string)
          db     (df/create-db (:schema config))
          conn   (df/debug-conn "ws://127.0.0.1:6262")]

      (doseq [[key query] (:queries config)]
        (df/exec! conn
          (df/register-query db (name key) query (:rules config))))

      (doseq [tx (:transact config)]
        (df/exec! conn (df/transact db tx))))))
