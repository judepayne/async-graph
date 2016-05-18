(ns async-graph.sandbox
  (:require [clojure.core.async :refer
             [>! <! >!! <!! go chan buffer close! thread alts! alts!!
              timeout mult tap untap pipeline]]))


;; How to use mult>>
(comment
  (def to-mult (chan 1)) ;; correspond to node channel
  (def m (mult to-mult)) ;; need to make a mult of it - this is a 'reify' type

  (let [c (chan 1)]
    (tap m c)
    (go (loop []
          (when-let [v (<! c)] ;; replace by while true pipeline (to next node)
            (println "Got! " v)
            (recur)))
        (println "Exiting!")))
  ;; I like the structure of this loop - it keeps listening until to-mult is closed

  (close! to-mult)
  )


(def f1 (map #(str % "f1->")))
(def f2 (map #(str % "f2->")))
(def f3 (map #(str % "f3->")))
(def f4 (map #(str % "f4->")))
(def f5 (map #(str % "f5->")))
(def f6 (map #(str % "f6->")))

(def g
  {f1 #{f3 f4}
   f2 #{f3 f5}
   f3 #{f6}
   f4 #{f6}
   f5 #{f6}})

(def simple-g
  {f1 #{f2}})

(def h
  {:a #{:c :d}
   :b #{:c :e}
   :c #{:f}
   :d #{:f}
   :e #{:f}})
