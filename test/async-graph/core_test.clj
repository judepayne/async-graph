(ns async-graph.core-test
  (:require [clojure.test :refer :all]
            [async-graph.core2 :refer :all]
            [async-graph.graph2 :refer :all]
            [clojure.core.async :refer
             [>! <! >!! <!! go chan buffer close! thread alts! alts!!
              timeout mult tap untap pipeline onto-chan]]))

;; set up some test data
(def fa (map #(str % "fa->")))
(def fb (map #(str % "fb->")))
(def fc (map #(str % "fc->")))
(def fd (map #(str % "fd->")))
(def fe (map #(str % "fe->")))
(def ff (map #(str % "ff->")))
(def fg (map #(str % "fg->")))
(def fh (map #(str % "fh->")))

(def nda [fa {:chdn #{fc fd} :buf-or-n 12 :ex-handler identity :label :inc}])
(def ndb [fb {:chdn #{} :buf-or-n 20 :ex-handler identity :label :dec}])
(def ndc [fb {:chdn #{} :buf-or-n 20 :parallelism 2 :label :dec}])
(def defaultss {:buf-or-n 10})
(def g-mini (into {} [nda ndb]))

(def g-midi
  {fa {:chdn #{fc fd} :buf 5}
   fb {:chdn #{fc fe} :buf 10}
   fc {:chdn #{ff} :buf 15}
   fd {:chdn #{ff} :buf 20}
   fe {:chdn #{ff} :buf 25}})

(def micro
  {fa {:chdn #{fb} :buf-or-n 15}})

(comment
  "test node creation. these functions will be made private so not directly
   testable"
  (clojure.pprint/pprint (enable-node (first nda) (second nda) :leaf? true))
  (clojure.pprint/pprint (enable-node (first nda) (second nda)))
  (clojure.pprint/pprint (enable-node (first ndc) (second ndc)))
  (clojure.pprint/pprint (normalize micro))
  (clojure.pprint/pprint (async-graph (normalize micro) {:buf-or-n 10}))
  )











