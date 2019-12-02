(ns scrollrack.core)

(require '[datomic.client.api :as d]
         '[ravencoin-rpc.core :as r])

; set up datomic client
(def d-cfg {:server-type :ion
            :region      "us-east-2"
            :system      "datomic-1"
            :endpoint    "http://entry.datomic-1.us-east-2.datomic.net:8182/"
            :proxy-port  8182})

(def d-client (d/client d-cfg))

; set up raven client
(def r-cfg {:url  "http://127.0.0.1:18766"
            :user "ravencoin"
            :pass "local321"})

(def r-client (r/client r-cfg))

; create database
(def db-name "raven")

(d/create-database d-client {:db-name db-name})

; get database connection
(def d-conn (d/connect d-client {:db-name db-name}))

; create schema
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


(d/transact d-conn {:tx-data raven-schema})

; etl data
; I'm sure there's a nice way to do this...
(defn assoc-unless-nil
  [map key val]
  (if val (assoc map key val) map))

(defn get-transaction-detail
  [txid]
  (try
    (let [hex (r/get-raw-transaction r-client {:txid txid})]
      (r/decode-raw-transaction r-client {:hexstring hex}))
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
         :block/time       (java.util.Date. (long (:time b)))
         :block/size       (:size b)
         :block/height     (:height b)
         :block/tx         (map #(translate-tx (get-transaction-detail %)) (:tx b))}]
    (-> t-b
        (assoc-unless-nil :block/previousblockhash (:previousblockhash b))
        (assoc-unless-nil :block/nextblockhash (:nextblockhash b)))))

; etl all blocks
(defn get-block-count
  []
  (inc (r/get-block-count r-client)))

(defn get-block-indexes
  ([]    (range 0 (get-block-count)))
  ([t]   (take t (get-block-indexes)))
  ([t d] (take t (drop d (get-block-indexes)))))

(defn get-block-hashes
  [& args]
  (map #(r/get-block-hash r-client {:height %}) (apply get-block-indexes args)))

(defn get-blocks
  [& args]
  (map #(r/get-block r-client {:blockhash %}) (apply get-block-hashes args)))

(defn translate-blocks
  [& args]
  (map translate-block (apply get-blocks args)))

(defn etl-blocks
  [& args]
  (d/transact d-conn {:tx-data (apply translate-blocks args)}))

; load the blocks for real!
;(etl-blocks)

; grab db
(def db (d/db d-conn))

; query
(def all-blocks-q '[:find (pull ?b [*])
                    :where [?b :block/hash]])

(d/q all-blocks-q db)

(defn fetch-block [n]
  (d/q '[:find (pull ?b [*])
         :in $ ?n
         :where [?b :block/height ?n]]
       (d/db d-conn) n))

; drop database
;(d/delete-database d-client {:db-name db-name})