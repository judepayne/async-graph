(ns async-graph.core-test
  (:require [clojure.test :refer :all]
            [async-graph.graph2 :refer [not-roots nodes roots normalize
                                        leaves not-leaves kahn-sorted edges
                                        parents-of root?]]))

(def g
  {7  {:chdn #{11 8} :buf 5}
   5  {:chdn #{11} :buf 6}
   3  {:chdn #{8 10} :buf 7}
   11 {:chdn #{2 9} :buf 8}
   8  {:chdn #{9} :buf 10}})

(def cyclic-g (assoc g 2 {:chdn #{11} :buf 'who-cares}))

(def g-norm (normalize ag))

(deftest simple-graph-ops
  (testing "nodes of the graph"
    (is (= #{7 3 11 5 8}) (nodes g)))
  (testing "roots of the graph"
    (is (= #{7 3 5} (roots g))))
  (testing "not-roots of the graph"
    (is (= #{2 11 9 10 8} (not-roots g))))
  (testing "leaves of the graph"
    (is (and (= #{} (leaves g))
             (= #{2 9 10} (leaves (normalize g))))))
  (testing "not-leaves of the graph"
    (is (and (= (not-leaves (normalize g)) (not-leaves g))
             (= #{7 3 11 5 8} (not-leaves g)))))
  (testing "edges of the graph"
    (is (and (= #{[8 9] [11 9] [11 2] [7 11] [7 8] [5 11] [3 10] [3 8]} (edges g))
             (= (edges (normalize g)) (edges g))))))

(deftest normalize-graph
  (testing "normalize function"
    (is (= #{7 3 2 11 9 5 10 8} (nodes (normalize g))))))

(deftest kahn
  (testing "kahn sorting a graph"
    (is (and (= '(7 3 5 11 2 10 8 9) (keys (kahn-sorted g)))
             (nil? (kahn-sorted cyclic-g))))))

(deftest node-ops
  (testing "orphaned/ root?"
    (is (and (not (root? g 10))
             (root? g 7))))
  (testing "parents-of"
    (is (and (= #{11 8} (parents-of g 9))
             (empty? (parents-of g 7))))))
