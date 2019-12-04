(ns scrollrack.core
  (:require [environ.core :refer [env]]
            [ravencoin-rpc.core :as r]
            [datomic.client.api :as d]))


;;;; THIS SECTION IS THE TOP LEVEL SETUP STUFF
; set up raven client
(def r-cfg {:url  (env :raven-url)
            :user (env :raven-user)
            :pass (env :raven-pass)})
(defn r-client [] (r/client r-cfg))

; set up datomic client
(def d-cfg {:server-type (keyword (env :datomic-server-type))
            :region      (env :datomic-region)
            :system      (env :datomic-system)
            :endpoint    (env :datomic-endpoint)
            :proxy-port  (Integer/parseInt (env :datomic-proxy-port))})
(defn d-client [] (d/client d-cfg))

; what database are we talkin' about here?
(def db-name (env :db-name))

; get database connection
(defn d-conn [] (d/connect (d-client) {:db-name db-name}))

; grab db
(defn db [] (d/db (d-conn)))


;;;; THIS SECTION IS SCHEMA STUFF
; create db
(defn create-database []
  (d/create-database (d-client) {:db-name db-name}))

; define schema
(def raven-schema [{:db/ident       :block/hash
                    :db/valueType   :db.type/string
                    :db/cardinality :db.cardinality/one
                    :db/unique      :db.unique/identity
                    :db/doc         "The block hash"}
                   {:db/ident       :block/version
                    :db/valueType   :db.type/long
                    :db/cardinality :db.cardinality/one
                    :db/doc         "The block version"}
                   {:db/ident       :block/difficulty
                    :db/valueType   :db.type/double
                    :db/cardinality :db.cardinality/one
                    :db/doc         "The block difficulty"}
                   {:db/ident       :block/time
                    :db/valueType   :db.type/instant
                    :db/cardinality :db.cardinality/one
                    :db/doc         "The block time"}
                   {:db/ident       :block/size
                    :db/valueType   :db.type/long
                    :db/cardinality :db.cardinality/one
                    :db/doc         "The block size"}
                   {:db/ident       :block/height
                    :db/valueType   :db.type/long
                    :db/cardinality :db.cardinality/one
                    :db/doc         "The block height"}
                   {:db/ident       :block/previousblockhash
                    :db/valueType   :db.type/string
                    :db/cardinality :db.cardinality/one
                    :db/doc         "The block previousblockhash"}
                   {:db/ident       :block/nextblockhash
                    :db/valueType   :db.type/string
                    :db/cardinality :db.cardinality/one
                    :db/doc         "The block nextblockhash"}
                   {:db/ident       :block/tx
                    :db/valueType   :db.type/ref
                    :db/isComponent true
                    :db/cardinality :db.cardinality/many
                    :db/doc         "The block's transactions (tx)"}

                   {:db/ident       :tx/txid
                    :db/valueType   :db.type/string
                    :db/cardinality :db.cardinality/one
                    :db/unique      :db.unique/identity
                    :db/doc         "The tx txid/hash"}
                   {:db/ident       :tx/vin
                    :db/valueType   :db.type/ref
                    :db/isComponent true
                    :db/cardinality :db.cardinality/many
                    :db/doc         "The tx's inputs (vin)"}
                   {:db/ident       :tx/vout
                    :db/valueType   :db.type/ref
                    :db/isComponent true
                    :db/cardinality :db.cardinality/many
                    :db/doc         "The tx's outputs (vin)"}

                   ; TODO: combo of [:in/txid :in/vout] should be unique...
                   {:db/ident       :in/coinbase
                    :db/valueType   :db.type/boolean
                    :db/cardinality :db.cardinality/one
                    :db/doc         "Whether this is a coinbase inputs"}
                   {:db/ident       :in/txid
                    :db/valueType   :db.type/string
                    :db/cardinality :db.cardinality/one
                    :db/doc         "The txid of the output being consumed"}
                   {:db/ident       :in/vout
                    :db/valueType   :db.type/long
                    :db/cardinality :db.cardinality/one
                    :db/doc         "The index of the output being consumed"}

                   ; TODO: constrain to enum [:rvn :issue :transfer :reissue (???)]
                   {:db/ident       :out/type
                    :db/valueType   :db.type/keyword
                    :db/cardinality :db.cardinality/one
                    :db/doc         "The output type (rvn, issue, transfer, reissue, etc.)"}
                   {:db/ident       :out/unit
                    :db/valueType   :db.type/string
                    :db/cardinality :db.cardinality/one
                    :db/doc         "The output unit (rvn or asset name)"}
                   {:db/ident       :out/qty
                    :db/valueType   :db.type/double
                    :db/cardinality :db.cardinality/one
                    :db/doc         "The output quantity"}
                   {:db/ident       :out/to
                    :db/valueType   :db.type/string
                    :db/cardinality :db.cardinality/one
                    :db/doc         "The address the output is to"}
                   {:db/ident       :out/n
                    :db/valueType   :db.type/long
                    :db/cardinality :db.cardinality/one
                    :db/doc         "The output's vout index"}])

; load schema
(defn define-schema []
  (d/transact (d-conn) {:tx-data raven-schema}))

; drop database
(defn delete-database []
  (d/delete-database d-client {:db-name db-name}))


;;;; THIS SECTION IS ETL STUFF
; etl data
; I'm sure there's a nice way to do this...
(defn assoc-unless-nil
  [map key val]
  (if val (assoc map key val) map))

(defn get-transaction-detail
  [txid]
  (try
    (let [hex (r/get-raw-transaction (r-client) {:txid txid})]
      (r/decode-raw-transaction (r-client) {:hexstring hex}))
    (catch Exception e
      (println "Couldn't get transaction detail for [" txid "]: " (.getMessage e))
      {:txid txid})))

(defn translate-vin
  "Convert rpc/json representation to schema edn."
  [vin]
  (let [t-vin {:in/coinbase (not (nil? (:coinbase vin)))}]
    (-> t-vin
        (assoc-unless-nil :in/txid (:txid vin))
        (assoc-unless-nil :in/vout (:vout vin)))))

(defn get-vout-type
  [vout]
  (let [mapping
        {"pubkey"         :rvn
         "pubkeyhash"     :rvn
         "nulldata"       :data
         "new_asset"      :issue
         "reissue_asset"  :reissue
         "transfer_asset" :transfer}]
    (get mapping (:type (:scriptPubKey vout)) :other)))

(defn get-vout-unit
  [vout]
  (or (get-in vout [:scriptPubKey :asset :name])
      (if (> (:value vout) 0.0) "rvn" nil)))

(defn translate-vout
  "Convert rpc/json representation to schema edn."
  [vout]
  (let [t-vout
        {:out/type (get-vout-type vout)
         :out/qty  (get-in vout [:scriptPubKey :asset :amount] (:value vout))
         :out/n    (:n vout)}]
    (-> t-vout
        (assoc-unless-nil :out/to (first (get-in vout [:scriptPubKey :addresses])))
        (assoc-unless-nil :out/unit (get-vout-unit vout)))))

(defn translate-tx
  "Convert rpc/json representation to schema edn."
  [tx]
  {:tx/txid (:txid tx)
   :tx/vin (map translate-vin (:vin tx))
   :tx/vout (map translate-vout (:vout tx))})

(defn translate-block
  "Convert rpc/json representation to schema edn."
  [b]
  (let [t-b
        {:block/hash       (:hash b)
         :block/version    (:version b)
         :block/difficulty (:difficulty b)
         :block/time       (java.util.Date. (* (long (:time b)) 1000))
         :block/size       (:size b)
         :block/height     (:height b)
         :block/tx         (map #(translate-tx (get-transaction-detail %)) (:tx b))}]
    (-> t-b
        (assoc-unless-nil :block/previousblockhash (:previousblockhash b))
        (assoc-unless-nil :block/nextblockhash (:nextblockhash b)))))

; etl all blocks
(defn get-block-count
  []
  (inc (r/get-block-count (r-client))))

(defn get-block-indexes
  ([]    (range 0 (get-block-count)))
  ([t]   (take t (get-block-indexes)))
  ([t d] (take t (drop d (get-block-indexes)))))

(defn get-block-hashes
  [& args]
  (map #(r/get-block-hash (r-client) {:height %}) (apply get-block-indexes args)))

(defn get-blocks
  [& args]
  (map #(r/get-block (r-client) {:blockhash %}) (apply get-block-hashes args)))

(defn translate-blocks
  [& args]
  (map translate-block (apply get-blocks args)))

(defn etl-blocks
  [& args]
  (d/transact (d-conn) {:tx-data (apply translate-blocks args)}))

; load the blocks for real!
;(etl-blocks)


;;;; THIS SECTION IS QUERY STUFF
; query
(def all-blocks-q '[:find (pull ?b [*])
                    :where [?b :block/hash]])

(def block-count-q '[:find (count ?b)
                     :where [?b :block/hash]])

(defn fetch-block [n]
  (d/q '[:find (pull ?b [*])
         :in $ ?n
         :where [?b :block/height ?n]]
       (d/db (d-conn)) n))

(defn out-type-count
  "Returns number of outputs of the specified type between block time t1 and t2."
  [out-type from-inst to-inst]
  (d/q '[:find (count ?out)
         :in $ ?type ?t1 ?t2
         :where
         [?block :block/time ?time]
         [(>= ?time ?t1)]
         [(< ?time ?t2)]
         [?block :block/tx ?tx]
         [?tx :tx/vout ?out]
         [?out :out/type ?type]]
       (db) out-type from-inst to-inst))

; times series queries
; a.k.a. the payoff?
(defn out-type-frequency
  " Answers questions like \"How many transfers per week happened in 2019?
  out-type can be :rvn, :issue, :transfer, :reissue
  scale is in seconds for now (so pass 60*60*24*7 for weeks)
  from-when and to-when are instants"
  [out-type scale from-when to-when]
  (map #(apply (partial out-type-count out-type) (map (fn [ms] (java.util.Date. ms)) %))
       (partition 2 1 [(.getTime to-when)]
                  (range (.getTime from-when) (.getTime to-when) scale))))
