(defproject scrollrack "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [environ "1.1.0"]
                 [ravencoin-rpc "0.1.0-SNAPSHOT"]
                 [com.datomic/client-cloud "0.8.78"]
                 [javax.xml.bind/jaxb-api "2.3.1"]]
  :repl-options {:init-ns scrollrack.core})
