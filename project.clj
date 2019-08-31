(defproject com.sixthnormal/clj-3df "0.1.0-alpha"
  :description "3DF client"
  :url "https://github.com/sixthnormal/clj-3df"
  :license {:name "Eclipse Public License"
            :url  "https://www.eclipse.org/legal/epl-v20.html"}

  :plugins [[lein-tools-deps "0.4.1"]]
  :middleware [lein-tools-deps.plugin/resolve-dependencies-with-deps-edn]
  :lein-tools-deps/config {:config-files ["deps.edn" :install :user :project]}

  :profiles {:dev {:dependencies [[cider/piggieback "0.3.9"]]
                   :repl-options {:nrepl-middleware [cider.piggieback/wrap-cljs-repl]}}}

  :main clj-3df.core)
