(defproject scrollrack "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 ; configuration
                 [environ "1.1.0"]
                 ; raven interface
                 [ravencoin-rpc "0.1.0-SNAPSHOT"]
                 ; datomic interface
                 [com.datomic/client-cloud "0.8.78"]
                 ; data data data
                 [javax.xml.bind/jaxb-api "2.3.1"]
                 [cheshire "5.9.0"]
                 ; web server stuff
                 [compojure "1.6.1"]
                 [http-kit "2.3.0"]
                 [ring/ring-defaults "0.3.2"]
                 [ring-cors "0.1.13"]
                 ; oz / vega-lite
                 [metasoarous/oz "1.5.6"]
                 ;[metasoarous/oz "1.6.0-alpha5"]
                 ]
  :main scrollrack.main
  :repl-options {:init-ns scrollrack.assets})
