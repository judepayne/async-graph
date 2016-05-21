(ns async-graph.graph2
  (:require [clojure.set :refer [difference union intersection]]))

;;------------------------------------------------------------------------
;; Usually a graph would be expressed as e.g.
;; {f1 #{f2 f3}
;;  f2 #{f4}
;;  f3 #{f4}}
;; but for this project, we need additional node metadata so enrich the
;; data format so a node is expressed like this
;; {f1 {:chdn #{f2 f3} :buf-or-n 5 :ex-handler my-ex-fn etc.. } ..
;; so the below library of graph functions have been amended to work
;; with an accessor, access and a function create for creating the
;; necessary new nodes non-destructively. Setting the below both to
;; identity will accomodate the original format.
(def access :chdn)
(def create (fn [n] {:chdn n}))
;;------------------------------------------------------------------------

(defn ^:private without
  "Returns set s with x removed."
  [s x] (difference s #{x}))

(defn ^:private without-access-s
  "Returns set s with x removed. uses access to get the set from s."
  [s x] (difference (access s) #{x}))


(defn ^:private take-1
  "Returns the pair [element, s'] where s' is set s with element removed."
  [s] {:pre [(not (empty? s))]}
  (let [item (first s)]
    [item (without s item)]))

;;------------------------------------------------------------------------
;; Whole graph functions
;;------------------------------------------------------------------------

(defn not-roots
  "Return the set of nodes which are not roots."
  [g]
  (apply union (map access (vals g))))

(defn nodes
  "Returns a list of the nodes in the graph"
  [g]
  (set (keys g)))

(defn roots
  "Returns the set of nodes in graph g for which there are no incoming
  edges, where g is a map of nodes to sets of nodes. accessor is a function
  that digs the set out of vals of the graph."
  [g]
  (difference (nodes g) (not-roots g)))

(defn normalize
  "Returns g with empty outgoing edges added for nodes with incoming
  edges only.  Example: {:a #{:b}} => {:a #{:b}, :b #{}}. accessor is a
  function that digs the set out of vals of the graph."
  [g]
  (reduce
   (fn [acc cur]
     (if (get acc cur)
       acc
       (assoc acc cur (create #{}))))
   g
   (not-roots g)))

(defn leaves
  "Returns the set of the leaves of the graph"
  [g]
  (set (keys (filter (fn [[k v]] (empty? (access v))) g))))

(defn not-leaves
  "Returns a set of the nodes in the graph which are not leaves"
  [g]
  (difference (nodes g) (leaves g)))

(defn ^:private kahn-sort
  "Proposes a topological sort for directed graph g using Kahn's
   algorithm, where g is a map of nodes to sets of nodes. If g is
   cyclic, returns nil."
  ([g]
     (kahn-sort (normalize g) [] (roots g)))
  ([g l s]
     (if (empty? s)
       (when (every? empty? (map access (vals g))) l)
       (let [[n s'] (take-1 s)
             m  (access (g n))
             g' (reduce #(update-in % [n] without-access-s %2) g m)]
         (recur g' (conj l n) (union s' (intersection (roots g') m)))))))

(defn kahn-sorted
  "Sort the graph using the kahn topological sort. Returns nil
   if the graph is cyclic."
  [g]
  (let [norm (normalize g)]
    (if-let [sorted-g (kahn-sort g)]
      (into {} (map #(vec [% (get norm %)]) sorted-g))
      nil)))

(defn edges
  "Returns the set of the edges in the graph where each edge is a
   vector of two nodes, the first node pointing at the second."
  [g]
  (set
   (reduce
    (fn [acc [k v]]
      (let [chdn (access v)]
        (concat acc (map vector (repeat k) chdn))))
    #{}
    g)))

;;------------------------------------------------------------------------
;; Node functions
;;------------------------------------------------------------------------

(defn ^:private incoming
  "DEPRECATED: returns a set of nodes with directed edges to the specified node."
  [g n]
  (set
    (filter
     #(let [[src tgt] %] (= tgt n))
     (edges g))))

(defn parents-of
  "Returns the set of the parents of the node."
  [g n]
  (let [nset #{n}]
    (reduce
     (fn [acc [k v]]
       (if (empty? (intersection nset (access v)))
         acc
         (conj acc k)))
     #{}
     g)))

(defn root?
  "Returns true if the node has no parents"
  [g n]
  (empty? (intersection (not-roots g) #{n})))


