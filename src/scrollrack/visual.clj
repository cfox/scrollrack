(ns scrollrack.visual
  (:require [scrollrack.core :as core]
            [oz.core :as oz]))

(defn recent-transfer-count
  []
  {:data      {:values (flatten (core/fetch-recent-outputs))}
   :transform [{:filter {"field" :out/type "equal" :transfer}}]
   :mark      "bar"
   :encoding  {:y {:field :out/unit
                   :type  :ordinal}
               :x {:aggregate "count"
                   :type      "quantitative"}}})

(defn recent-transfer-qtys
  []
  {:data      {:values (flatten (core/fetch-recent-outputs))}
   :transform [{:filter {"field" :out/type "equal" :transfer}}]
   :mark      "bar"
   :encoding  {:y {:field :out/unit
                   :type  :ordinal}
               :x {:aggregate "sum"
                   :field :out/qty
                   :type      "quantitative"}}})

(defn recent-transfer-rpt
  []
  [:div
   [:h1 "10 minutes of transfer outputs..."]
   [:div {:style {:display "flex" :flex-direction "row"}}
    [:vega-lite (recent-transfer-count) {:height 400 :width 500}]
    [:vega-lite (recent-transfer-qtys) {:height 400 :width 500}]]])
