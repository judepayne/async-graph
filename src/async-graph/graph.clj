(ns async-graph.graph
  (:require [clojure.set :refer [difference union intersection]]))

(use 'async-graph.sandbox)

(defn map-vals [m f]
          (into {} (for [[k v] m]
                     [k (f v)])))

(defn without
  "Returns set s with x removed."
  [s x] (difference s #{x}))

(defn take-1
  "Returns the pair [element, s'] where s' is set s with element removed."
  [s] {:pre [(not (empty? s))]}
  (let [item (first s)]
    [item (without s item)]))

(defn roots
  "Returns the set of nodes in graph g for which there are no incoming
  edges, where g is a map of nodes to sets of nodes."
  [g]
  (let [nodes (set (keys g))
        have-incoming (apply union (vals g))]
    (difference nodes have-incoming)))

(defn normalize
  "Returns g with empty outgoing edges added for nodes with incoming
  edges only.  Example: {:a #{:b}} => {:a #{:b}, :b #{}}"
  [g]
  (let [have-incoming (apply union (vals g))]
    (reduce #(if (get % %2) % (assoc % %2 #{})) g have-incoming)))

(defn kahn-sort
  "Proposes a topological sort for directed graph g using Kahn's
   algorithm, where g is a map of nodes to sets of nodes. If g is
   cyclic, returns nil."
  ([g]
     (kahn-sort (normalize g) [] (roots g)))
  ([g l s]
     (if (empty? s)
       (when (every? empty? (vals g)) l)
       (let [[n s'] (take-1 s)
             m (g n)
             g' (reduce #(update-in % [n] without %2) g m)]
         (recur g' (conj l n) (union s' (intersection (roots g') m)))))))

(defn edges
  "Returns a set of the edges in the graph where each edge is a
   vector of two nodes, the first node pointing at the second."
  [g]
  (set
   (reduce
    (fn [acc curr]
      (let [[node children] curr]
        (concat acc (map vector (repeat node) children))))
    #{}
    g)))

(defn incoming
  "returns a set of nodes with directed edges to the specified node."
  [g node]
  (set
    (filter
     #(let [[src tgt] %] (= tgt node))
     (edges g))))

(defn kahn-sort-graph
  "Sort the graph using the kahn topological sort. Returns nil
   if the graph is cyclic."
  [g]
  (let [norm (normalize g)]
    (if-let [sorted-g (kahn-sort g)]
      (into {} (map #(vec [% (get norm %)]) sorted-g))
      nil)))

(defn leaves
  "Returns the leaves of the graph"
  [g]
  (set (keys (filter (fn [[k v]] (empty? v)) (normalize g)))))

(defn non-leaves
  "Returns the nodes in the graph which are not leaves"
  [g]
  (difference (set (keys (normalize g))) (leaves g)))

(defn nodes
  "Returns a list of the nodes in the graph"
  [g]
  (keys (normalize g)))

(defn deps
  "Lazily returns the list of nodes included & downstream of the specified.
   Implemtned as a depth first search alogrithm"
  [g node]
  ((fn rec-dfs [explored frontier]
     (lazy-seq
       (if (empty? frontier)
         nil
         (let [v (last frontier)
               neighbors (g v)]
           (cons v (rec-dfs
                   (into explored neighbors)
                   (into (pop frontier) (remove explored neighbors))))))))
   #{node} [node]))

(defn parents
  "Returns a list of the parents of the node."
  [g n]
  (let [nset (set [n])]
    (reduce
     (fn [acc [k v]]
       (if (empty? (intersection nset v))
         acc
         (conj acc k)))
     []
     g)))

(defn no-parent?
  "Returns true if the node has no parents"
  [g n]
  (empty? (intersection (apply union (vals g)) #{n})))

(comment
  (def acyclic-g
    {7 #{11 8}
     5 #{11}
     3 #{8 10}
     11 #{2 9}
     8 #{9}})

  (def cyclic-g
    {7 #{11 8}
     5 #{11}
     3 #{8 10}
     11 #{2 9}
     8 #{9}
     2 #{11}}) ;oops, a cycle!

  (kahn-sort acyclic-g) ;=> [3 5 7 8 10 11 2 9]
  (kahn-sort cyclic-g) ;=> nil


  
  )
