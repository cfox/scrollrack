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
                    :db/unique      :db.unique/identity
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


;;;; THIS SECTION IS QUERY STUFF
; query
(defn fetch-block-count
  []
  (ffirst (d/q '[:find (count ?b)
                 :where [?b :block/hash]]
               (d/db (d-conn)))))

(defn fetch-block [n]
  (ffirst (d/q '[:find (pull ?b [*])
                 :in $ ?n
                 :where [?b :block/height ?n]]
               (d/db (d-conn)) n)))

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
  "Get the (longest chain) block count from Raven."
  []
  (inc (r/get-block-count (r-client))))

(defn get-block
  "Get block at height n from Raven."
  [n]
  (r/get-block (r-client) {:blockhash (r/get-block-hash (r-client) {:height n})}))

(defn get-block-indexes
  "Get the (longest chain's) block indexes from Raven (e.g. [0 1 2 3]).
  Provide `t` and `d` to take/drop values for pagination/batching."
  ([]    (range 0 (get-block-count)))
  ([t]   (take t (get-block-indexes)))
  ([t d] (take t (drop d (get-block-indexes)))))

(defn get-block-hashes
  "Get the (longest chain's) block hashes from Raven.
  Provide `t` and `d` to take/drop values for pagination/batching."
  [& args]
  (map #(r/get-block-hash (r-client) {:height %}) (apply get-block-indexes args)))

(defn get-blocks
  "Get block info via `ravencoin-rpc.core/get-block` from Raven.
  Provide `t` and `d` to take/drop values for pagination/batching."
  [& args]
  (map #(r/get-block (r-client) {:blockhash %}) (apply get-block-hashes args)))

(defn translate-blocks
  "Extract blocks from Raven and transform them to schema-conforming EDN.
  Provide `t` and `d` to take/drop values for pagination/batching."
  [& args]
  (map translate-block (apply get-blocks args)))

(defn etl-blocks
  "Extract blocks from Raven, transform them and assert them in a transaction.
  Provide `t` and `d` to take/drop values for pagination/batching."
  [& args]
  (d/transact (d-conn) {:tx-data (apply translate-blocks args)}))

; load the blocks for real!
;(etl-blocks)

; assets active mainnet block: 435522 (use 435500)

; ETL; we want it to be:
; -- algo:
;    -- determine state (`r` is Raven chain; `d` is Datomic "chain")
;    -- r and d same size with same tip hash -- up to date!
;    -- r smaller than d: find last common block and roll back d
;    -- r bigger than d: find last common block and assert rest of r
;    -- (a full consistency check would be nice too sometimes -- all hashes match, etc.)
(def spock (agent {:status :unknown}))
(defn meld-chunk
  "Given an agent state, meld at most n blocks and return the updated state."
  [state n]
  (let [r-count (get-block-count)
        r-tip (:hash (get-block (dec r-count)))
        d-count (fetch-block-count)
        d-tip (:block/hash (fetch-block (dec d-count)))
        s (assoc state :status :melding
                       :r-count r-count :r-tip r-tip
                       :d-count d-count :d-tip d-tip)]
    (if (and (= r-count d-count) (= r-tip d-tip))
      (assoc s :status :ok)
      (if (< d-count r-count)
        (let [batch-size (min n (- r-count d-count))]
          (etl-blocks batch-size d-count)
          (let [new-d-count (fetch-block-count)
                new-d-tip (:block/hash (fetch-block (dec d-count)))]
            (assoc s :status :loading
                     :d-count new-d-count
                     :d-tip new-d-tip)))
        (assoc s :status :ahead)))))
(defn meld
  "Meld blockchain from Raven to Datomic.
  Idempotent, incremental, and non-blocking with a single worker thread.
  Keep calling this and eventually you'll be up to date!
  Returns a status and some counts (tbd)."
  []
  (deref (send-off spock meld-chunk 100)))

(defn spot-check
  "Get the hash of a random block from Datomic and make sure it matches
  the block with the same index on Raven."
  []
  (let [n (rand-int (fetch-block-count))
        r-hash (:hash (get-block n))
        d-hash (:block/hash (fetch-block n))]
    (assert (= r-hash d-hash))))
; TODO: VALIDATE function to check all block hashes?

