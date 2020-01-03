(ns scrollrack.main
  (:require [environ.core :refer [env]]
            [scrollrack.core :as core]
            [org.httpkit.server :as server]
            [compojure.core :refer :all]
            [compojure.route :as route]
            [ring.middleware.defaults :refer :all]
            [ring.middleware.cors :refer [wrap-cors]]
            [clojure.pprint :as pp]
            [clojure.string :as str]
            [cheshire.core :refer :all])
  (:gen-class))

; Simple Body Page
(defn simple-body-page [req]
  {:status  200
   :headers {"Content-Type" "text/html"}
   :body    "Hello World"})

; request-example
(defn request-example [req]
  {:status  200
   :headers {"Content-Type" "text/html"}
   :body    (->>
              (pp/pprint req)
              (str "Request Object: " req))})

(defn fetch-block [req]
  {:status  200
   :headers {"Content-Type" "application/json"}
   :body    (->>
              (generate-string (core/fetch-block (Integer/parseInt (:n (:params req))))))})

(defroutes app-routes
           (GET "/" [] simple-body-page)
           (GET "/request" [] request-example)
           (GET "/fetch-block" [] fetch-block)
           (route/not-found "Error, page not found!"))

(def app
  (-> app-routes
      (wrap-defaults site-defaults)
      (wrap-cors :access-control-allow-origin [#".*"]
                 :access-control-allow-headers ["Content-Type"]
                 :access-control-allow-methods [:get :put :post :delete :options])))

(defn -main
  "This is our main entry point"
  [& args]
  (let [port (Integer/parseInt (or (System/getenv "PORT") "3000"))]
    (server/run-server app {:port port})
    (println (str "Running webserver at http://127.0.0.1:" port "/"))))
