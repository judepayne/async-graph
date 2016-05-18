(ns small-stream.core-test
  (:require [clojure.test :refer :all]
            [small-stream.async-graph :refer :all]
            [clojure.core.async :refer
             [>! <! >!! <!! go chan buffer close! thread alts! alts!!
              timeout mult tap untap pipeline onto-chan]]))

;; set up some test data
(def f1 (map #(str % "f1->")))
(def f2 (map #(str % "f2->")))
(def f3 (map #(str % "f3->")))
(def f4 (map #(str % "f4->")))
(def f5 (map #(str % "f5->")))
(def f6 (map #(str % "f6->")))
(def f7 (map #(str % "f7->")))
(def f8 (map #(str % "f8->")))

(def g
  {f1 #{f3 f4}
   f2 #{f3 f5}
   f3 #{f6}
   f4 #{f6}
   f5 #{f6}})

(def simple-g
  {f1 #{f2}})


(def g-live (async-graph g))

(def simple-g-live (async-graph simple-g))

(deftest creation
  (testing "async graph creation 1"
    (is (and (= (-> g-live first first) f1)
             (= (-> g-live second first) f2)
             )))
  (testing "async graph creation: graph is normalized. count of graph same as channels"
    (is (and (= (count g-live) 6)
             (= (count (.graph g-live)) 6)
             (= (count (.channels g-live)) 6)
             ))))


