(ns async-graph.core-test
  (:require [clojure.test :refer :all]
            [async-graph.core :refer :all]
            [clojure.core.async :refer
             [>! <! >!! <!! go chan buffer close! thread alts! alts!!
              timeout mult tap untap pipeline onto-chan]]))

(defn prn-thru
  ([x] (prn-thru "" x))
  ([sender x] (println sender ": " x) x))

;; set up some test data
(def f1 (map inc))
(def f2 (map dec))
(def f3 (map inc))
(def f4 (map inc))
(def f5 (map dec))
(def f6 (map inc))
(def f7 (map dec))
(def f8 (map inc))
(def f9 (map dec))
(def f10 (map inc))

(def nd1 [f1 {:chdn #{f2 f3} :buf-or-n 12 :ex-handler identity :label :inc}])
(def nd2 [f2 {:chdn #{} :buf-or-n 20 :ex-handler identity :label :dec}])
(def defs {::buf-or-n default-buffer})
(def g (into {} [nd1 nd2]))

;; f1 -> f2, f3
;; f2 -> {}
;; f3 -> {}

 (def g10 
  {f1 {::chdn #{f3 f4} ::buf-or-n 12 ::ex-handler identity}
   f2 {::chdn #{f4 f5} ::buf-or-n 12 ::ex-handler identity}
   f3 {::chdn #{f6 f7} ::buf-or-n 12 ::ex-handler identity}
   f4 {::chdn #{f7 f8 f9} ::buf-or-n 12 ::ex-handler identity}
   f5 {::chdn #{f9 f10} ::buf-or-n 12 ::ex-handler identity}})

(def g10-lite
  {f1 {::chdn #{f3 f4}}
   f2 {::chdn #{f4 f5}}
   f3 {::chdn #{f6 f7}}
   f4 {::chdn #{f7 f8 f9}}
   f5 {::chdn #{f9 f10}}})



(comment
  "test node creation. these functions will be made private so not directly
   testable"
  (clojure.pprint/pprint (enable-node (first nda) (second nda) :leaf? true))
  (clojure.pprint/pprint (enable-node (first nda) (second nda)))
  (clojure.pprint/pprint (enable-node (first ndc) (second ndc)))
  (clojure.pprint/pprint (normalize micro))
  (clojure.pprint/pprint (async-graph (normalize micro) {:buf-or-n 10}))
  )











