(ns async-graph.graph-test
  (:require [clojure.test :refer :all]
            [async-graph.graph :refer [not-roots nodes roots normalize
                                        leaves not-leaves kahn-sorted edges
                                        parents-of root? dfs bfs bfs-multi scc]]))

(def g
  {7  {:chdn #{11 8} :buf 5}
   5  {:chdn #{11} :buf 6}
   3  {:chdn #{8 10} :buf 7}
   11 {:chdn #{2 9} :buf 8}
   8  {:chdn #{9} :buf 10}})

(def g-disconnected
  {7  {:chdn #{11} :buf 5}
   5  {:chdn #{11} :buf 6}
   3  {:chdn #{10} :buf 7}
   11 {:chdn #{2 9} :buf 8}})

(def cyclic-g (assoc g 2 {:chdn #{11} :buf 'who-cares}))

(def g-norm (normalize g))

(def g10 (normalize
          {1 {:chdn #{3 4}}
           2 {:chdn #{4 5}}
           3 {:chdn #{6 7}}
           4 {:chdn #{7 8 9}}
           5 {:chdn #{9 10}}}))

(defn find-in
  "go to x in ys otherwise return nil"
  [x ys]
  (if (empty? ys)
    nil
    (if (= x (first ys))
      (rest ys)
      (find-in x (rest ys)))))

(defn same-order?
  "Returns true if the sequence xs contains elements in the same order
   as the (larger) sequence ys. Note that items in xs may appear multiple
   times in ys - as long as some selection appears in the same order as
   xs, will return true."
  [xs ys]
  (if (empty? xs)
    true
    (let [next (find-in (first xs) ys)]
      (if (nil? next)
        false
        (same-order? (rest xs) next)))))

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

(deftest dfs-bfs
  (testing "dfs"
    (is (= [5 11 9 2] (dfs g-norm 5))))
  (testing "bfs"
    (is (= '(5 11 2 9) (bfs g-norm 5))))
  (testing "dfs undirected"
    (is (= [5 11 9 8 3 10 2 7] (dfs g 5 :undirected? true))))
  (testing "bfs undirected"
    (is (= '(5 11 7 2 9 8 3 10) (bfs g 5 :undirected? true))))
  (testing "bfs-multi"
    (is (= '(2 4 5 7 9 8 10) (bfs-multi g10 [2 5])))))

(deftest strongly-connected-components
  (testing "connected graph"
    (is (= #{#{7 3 2 11 9 5 10 8}} (scc g))))
  (testing "disconnected graph"
    (is (= #{#{3 10} #{7 2 11 9 5}} (scc g-disconnected)))))
