(ns clj-3df.core
  (:refer-clojure :exclude [resolve])
  (:require
   #?(:clj  [clojure.core.async :as async :refer [<! >! >!! <!! go go-loop]]
      :cljs [cljs.core.async :as async :refer [<! >!]])
   #?(:clj  [clojure.spec.alpha :as s]
      :cljs [cljs.spec.alpha :as s])
   #?(:clj  [aleph.http :as http])
   #?(:clj  [manifold.stream :as stream])
   #?(:clj  [cheshire.core :as json])
   #?(:cljs [clj-3df.socket :as socket])
   [clojure.pprint :as pprint]
   [clojure.string :as str]
   [clojure.set :as set]
   [clj-3df.compiler :as compiler]
   [clj-3df.encode :as encode])
  #?(:cljs (:require-macros [cljs.core.async.macros :refer [go-loop]]
                            [clj-3df.core :refer [exec!]])))

;; HELPER

(defn parse-json [json]
  #?(:clj  (json/parse-string json)
     :cljs (js->clj (.parse js/JSON json))))

(defn stringify [obj]
  #?(:clj (cheshire.core/generate-string obj)
     :cljs (.stringify js/JSON (clj->js obj))))

(defprotocol IDB
  (-schema [db])
  (-attrs-by [db property])
  (-id->attribute [db id]))

(defrecord DB [schema rschema next-tx]
  IDB
  (-schema [db] (.-schema db))
  (-attrs-by [db property] ((.-rschema db) property)))

(defn- ^Boolean is-attr? [^DB db attr property] (contains? (-attrs-by db property) attr))
(defn- ^Boolean multival? [^DB db attr] (is-attr? db attr :db.cardinality/many))
(defn- ^Boolean ref? [^DB db attr] (is-attr? db attr :db.type/ref))
(defn- ^Boolean reverse-ref? [attr] (= \_ (nth (name attr) 0)))

(defn attr->properties [k v]
  (case v
    :db.unique/identity  [:db/unique :db.unique/identity :db/index]
    :db.unique/value     [:db/unique :db.unique/value :db/index]
    :db.cardinality/many [:db.cardinality/many]
    :db.type/ref         [:db.type/ref :db/index]
    :db.type/derived     [:db.type/derived]
    (when (true? v)
      (case k
        :db/isComponent [:db/isComponent]
        :db/index       [:db/index]
        []))))

(defn- rschema [schema]
  (reduce-kv
    (fn [m attr keys->values]
      (reduce-kv
        (fn [m key value]
          (reduce
            (fn [m prop]
              (assoc m prop (conj (get m prop #{}) attr)))
            m (attr->properties key value)))
        m keys->values))
    {} schema))

(defn create-db [schema]
  (->DB schema (rschema schema) 0))

(defn interest [name]
  [{:Interest {:name name}}])

(defn uninterest [name]
  [{:Uninterest name}])

(defn flow [source-name sink-name]
  [{:Flow [source-name sink-name]}])

(defn register [^DB db name plan rules]
  (let [;; @TODO expose this directly?
        ;; the top-level plan is just another rule...
        top-rule {:name name :plan plan}]
    [{:Register
      {:publish [name]
       :rules   (encode/encode-rules (conj rules top-rule))}}]))

(defn register-query
  ([^DB db name query] (register-query db name query []))
  ([^DB db name query rules]
   (let [;; @TODO expose this directly?
         ;; the top-level plan is just another rule...
         top-rule       {:name name :plan (compiler/compile-query query)}
         compiled-rules (if (empty? rules)
                          []
                          (compiler/compile-rules rules))]
     [{:Register
       {:publish [name]
        :rules   (encode/encode-rules (conj compiled-rules top-rule))}}])))

(defn query
  ([^DB db name q] (query db name q []))
  ([^DB db name q rules]
   (concat
    (register-query db name q rules)
    (interest name))))

(defn register-source [source]
  [{:RegisterSource source}])

(defn register-sink [name sink]
  [{:RegisterSink
    {:name name
     :sink sink}}])

(defn create-attribute [attr semantics]
  [{:CreateAttribute
    {:name   (encode/encode-keyword attr)
     :config {:trace_slack     {:TxId 1}
              :input_semantics (encode/encode-semantics semantics)}}}])

(defn create-db-inputs [^DB db]
  (->> (seq (.-schema db))
       (mapcat (fn [[name properties]]
                 (let [semantics (get properties :db/semantics :db.semantics/raw)]
                   (create-attribute name semantics))))))

(defn advance-domain [next-t]
  [{:AdvanceDomain [nil {:TxId next-t}]}])

(defn close-input [attr]
  [{:CloseInput (encode/encode-keyword attr)}])

(defn- reverse-ref [attr]
  (if (reverse-ref? attr)
    (keyword (namespace attr) (subs (name attr) 1))
    (keyword (namespace attr) (str "_" (name attr)))))

(defn- explode [^DB db entity]
  (let [eid (:db/id entity)]
    (for [[a vs] entity
          :when  (not= a :db/id)
          :let   [reverse?   (reverse-ref? a)
                  straight-a (if reverse? (reverse-ref a) a)
                  _          (when (and reverse? (not (ref? db straight-a)))
                               (throw
                                (ex-info "Reverse attribute name requires {:db/valueType :db.type/ref} in schema."
                                         {:error :transact/syntax, :attribute a, :context {:db/id eid, a vs}})))]
          v      (if (multival? db a) vs [vs])]
      (if (and (ref? db straight-a) (map? v)) ;; another entity specified as nested map
        (assoc v (reverse-ref a) eid)
        (if reverse?
          [:db/add v straight-a eid]
          [:db/add eid straight-a v])))))

(defn transact
  ([^DB db tx-data] (transact db nil tx-data))
  ([^DB db tx tx-data]
   (let [schema    (-schema db)
         op->diff  (fn [op]
                     (case op
                       :db/add     1
                       :db/retract -1))
         wrap-type (fn [a v]
                     (let [type (get-in schema [a :db/valueType] :db.type/unknown)]
                       (if (= type :db.type/unknown)
                         (throw (ex-info "Unknown value type" {:type type}))
                         {type v})))
         tx-data   (reduce (fn [tx-data datum]
                             (cond
                               (map? datum)
                               (->> (explode db datum)
                                    (transact db tx)
                                    first
                                    :Transact
                                    (into tx-data))

                               (sequential? datum)
                               (let [[op e a v] datum]
                                 (conj tx-data [(op->diff op) e (encode/encode-keyword a) (wrap-type a v)]))))
                           [] tx-data)]
     [{:Transact tx-data}])))

(defrecord Connection [ws out middleware pub])

(defn parse-result
  [result]
  (let [unwrap-type  (fn [boxed] (second (first boxed)))
        unwrap-tuple (fn [[tuple time diff :as result-diff]]
                       (if (vector? tuple)
                         [(mapv unwrap-type tuple) time diff]
                         result-diff))
        xf-batch     (map unwrap-tuple)]
    (let [[query_name results] (parse-json result)]
      [query_name (into [] xf-batch results)])))

(def xf-parse
  (map parse-result))

#?(:clj (defn- create-middleware
          "subs to ws changes and pushes them to out. Applies f on ws changes."
          [ws out f]
          (Thread.
           (fn []
             (println "[MIDDLEWARE] running")
             (loop []
               (when-let [result @(stream/take! ws ::drained)]
                 (if (= result ::drained)
                   (do
                     (println "[MIDDLEWARE] server closed connection")
                     (async/close! out))
                   (do
                     (when (some? f) (f result))
                     (>!! out result)
                     (recur)))))))))

#?(:cljs (defn- create-middleware
          "subs to ws changes and pushes them to out. Applies f on ws changes."
           [ws out f]
           (println "[MIDDLEWARE] running")
           (go-loop []
             (when-let [result (<! (:source ws))]
               (if (= result :drained)
                 (do
                   (println "[SUBSCRIBER] server closed connection")
                   (async/close! out))
                 (do
                   (when (some? f) (f result))
                   (>! out result)
                   (recur)))))))

#?(:clj (defn create-conn
          ([url] (create-conn url nil nil))
          ([url middleware] (create-conn url middleware nil))
          ([url middleware options]
           (let [ws  @(http/websocket-client url)
                 out (:channel options (async/chan 100))
                 mw  (create-middleware ws out middleware)]
             (.start mw)
             (->Connection ws out mw nil)))))

#?(:cljs (defn create-conn
           ([url] (create-conn url nil nil))
           ([url middleware] (create-conn url middleware nil))
           ([url middleware options]
            (let [ws  (socket/connect url)
                  out (:channel options (async/chan 100))
                  mw  (create-middleware ws out middleware)]
              (->Connection ws out mw nil)))))

(defn create-debug-conn [url]
  (create-conn url (comp pprint/pprint parse-result)))

(defn create-publication
  ([url] (create-publication url nil))
  ([url middleware]
   (let [^Connection conn (create-conn url middleware)]
     (assoc conn :pub (async/pub (:out conn) first)))))

(defn create-debug-publication [url]
  (create-publication url (fn [result] (println result))))

(defn exec-raw! [conn requests]
  (->> requests
       (stringify)
       #?(:clj (stream/put! (.-ws conn)))
       #?(:cljs (>! (:sink (.-ws conn))))))

(defn- if-cljs [env then else]
  (if (:ns env) then else))

#?(:clj (defmacro exec! [^Connection conn & forms]
          (if-cljs &env
                   (let [c   (gensym)
                         out (gensym)]
                     `(let [~c   ~conn
                            ~out (.-out ~c)]
                        (cljs.core.async/go
                          (do ~@(for [form forms]
                                (cond
                                  (nil? form) (throw (ex-info "Nil form within execution." {:form form}))
                                  (seq? form)
                                  (case (first form)
                                    'expect-> `(clojure.core/as-> (cljs.core.async/<! ~out) ~@(rest form))
                                    `(clojure.core/->> ~form (clj-3df.core/stringify) (cljs.core.async/>! (:sink (.-ws ~c)))))))))))
                   (let [c   (gensym)
                         out (gensym)]
                     `(let [~c   ~conn
                            ~out (.-out ~c)]
                        (do ~@(for [form forms]
                                (cond
                                  (nil? form) (throw (ex-info "Nil form within execution." {:form form}))
                                  (seq? form)
                                  (case (first form)
                                    'expect-> `(clojure.core/as-> (<!! ~out) ~@(rest form))
                                    `(clojure.core/->> ~form (clj-3df.core/stringify) (stream/put! (.-ws ~c))))))))))))

(defn business-rule
  "registers a query. Subscribes to its messages and calls callback with them."
  [^Connection conn ^DB db name query callback]
  (when (nil? (:pub conn)) (throw (ex-info "Business rule registered on non-pub connection" conn)))
  (let [c (async/chan)
        _ (async/sub (:pub conn) name c)]
    (exec-raw! conn
      (query db name query))
    (go-loop []
      (when-let [msg (<! c)]
        (callback (second msg))
        (recur)))))

(comment

  ;; pick one, for business rules, pick a *-publication conn
  (def conn (create-conn "ws://127.0.0.1:6262"))
  (def conn (create-debug-conn "ws://127.0.0.1:6262"))
  (def conn (create-conn "ws://127.0.0.1:6262" (fn [r] (println "ghetto mw"))))
  (def conn (create-publication "ws://127.0.0.1:6262"))
  (def conn (create-debug-publication "ws://127.0.0.1:6262"))
  (def conn (create-publication "ws://127.0.0.1:6262" (fn [r] (println "pub mw" r))))

  (def schema
    {:loan/amount  {:db/valueType :Number}
     :loan/from    {:db/valueType :String}
     :loan/to      {:db/valueType :String}
     :loan/over-50 {:db/valueType :Bool}})
  
  (def db (create-db schema))

  (exec! conn (create-db-inputs db))

  (def loans
    "an overview of all loans in the system"
    '[:find ?loan ?from ?amount ?to ?over-50
      :where
      [?loan :loan/amount ?amount]
      [?loan :loan/from ?from]
      [?loan :loan/to ?to]
      [?loan :loan/over-50 ?over-50]])
  
  (exec! conn (query db "loans" loans))

  (exec! conn (transact db [[:db/add 1 :loan/amount 100] [:db/add 1 :loan/from "A"] [:db/add 1 :loan/to "B"] [:db/add 1 :loan/over-50 false]]))

  (def loans>50
    "marks loans that are > 50"
    '[:find ?loan ?amount
      :where
      [?loan :loan/amount ?amount]])
  
  (business-rule conn db "loans>50" loans>50
    (fn [diffs]
      (println "executing rule loans>50")
      (doseq [[[id amount] op] diffs]
        (when (pos? op)
          (exec! conn (transact db [[:db/retract id :loan/over-50 false]]))
          (exec! conn (transact db [[:db/add id :loan/over-50 (> amount 50)]]))))))

  (exec! conn (transact db [{:db/id 2 :loan/amount 200 :loan/from "B" :loan/to "A" :loan/over-50 false}]))

  (exec! conn (transact db [[:db/retract 1 :loan/amount 100]]))
  )

