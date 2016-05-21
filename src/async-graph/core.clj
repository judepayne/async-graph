(ns async-graph.core
  (:require [clojure.core.async :refer
             [>! <! >!! <!! go chan buffer close! thread alts! alts!!
              timeout mult tap untap pipeline onto-chan]]
            [async-graph.graph :refer
             [normalize kahn-sort-graph roots edges leaves non-leaves nodes
              parents no-parent?]]
            [clojure.set :refer [difference union intersection]]))

(use 'async-graph.sandbox)

(defn map-vals [m f]
          (into {} (for [[k v] m]
                     [k (f v)])))

(def default-buffer 10)

(defn- make-chan
  "Creates a channel for the transducer xf and with the buffer buf. If the transducer
   carries :buf metadata, that values overrides buf."
  [buf xf]
  (if-let [meta-buf (:buf (meta xf))]
    (chan meta-buf xf)
    (chan buf xf)))

(defn graph-channels
  "From a graph of transducers creates a series of core.async channels which
   are connected as per the structure of the graph. Returns a map of transducer
   to a vec of its created [in-channel out-channel] where out-channel is a mult.
   g is assumed to be pre-normalised"
  [g buf]
  (let [chls (into {} (map (fn [[k v]] [k (make-chan buf k)]) g))
        ;; create the mults. leaves don't need mults
        mlts (into {} (map (fn [[k v]] [k (mult v)]) (apply dissoc chls (leaves g))))
        es (edges g)]
    ;; create taps between the nodes' out channels and children nodes.
    (doseq [e es] (tap (get mlts (first e)) (get chls (second e))))
    (into {} (for [[k v] chls]
               [k {:channel v :mult (get mlts k)}]))))


(defprotocol AccessChannels
  "For returning input and output core.async channels of an AsyncGraph"
  (ins [g] "The input channels of the Async Graph")
  (outs [g] "The output channels of the Async Graph"))

(declare async-graph-intl graph-assoc graph-dissoc)


;; Implement all necessary protocols for AsyncGraph to behave as a Map
(deftype AsyncGraph [graph channels buffer]

  clojure.lang.IPersistentMap
  (assoc [_ k v]
    (apply async-graph-intl
           (conj
            (vec (graph-assoc graph channels k v buffer)) buffer)))
  (assocEx [_ k v]
    (throw (Exception.))) ;; apparently now unused
  (without [_ k]
    (do (println (graph-dissoc graph channels k buffer))
        (async-graph-intl graph channels buffer)))

  java.lang.Iterable
  (iterator [_]
    (.iterator (kahn-sort-graph graph)))

  clojure.lang.Associative
  (containsKey [_ k]
    (.containsKey graph k))
  (entryAt [_ k]
    (.entryAt graph))

  clojure.lang.IPersistentCollection
  (count [_]
    (.count graph))
  (cons [_ [k v]]
    (apply async-graph-intl
           (conj
            (vec (graph-assoc graph channels k v buffer)) buffer)))
  (empty [_]
    (.empty graph))
  (equiv [this o]
    (and (isa? (class o) AsyncGraph)
         (.equiv this o)))

  clojure.lang.Seqable
  (seq [_]
    (.seq graph))  

  clojure.lang.ILookup
  (valAt [_ k]
    (.valAt graph k))
  (valAt [_ k not-found]
    (.valAt graph k not-found))

  AccessChannels
  (ins [_]
    (into {} (map (fn [r] [r (:channel (get channels r))]) (roots graph))))
  (outs [_]
    (into {} (map (fn [l] [l (:channel (get channels l))]) (leaves graph))))
  )

(defn async-graph
  "Returns an instance of AsyncGraph given a graph of transducers, g and optionally
   a core.async buffer or buffer integer to be applied to each channel in the
   returned AsyncGraph.
   g should be specified in a clojure map as follows (for example):
   {xf1 #{xf2 xf3}
    xf2 #{xf3}
   i.e. a map of nodes to set of children. leaf nodes, e.g. xf3 #{} needn't be specified.
   The graph g must not have any cycles.
   A transducer node may optional have metadata tags :buf-or-n <core.async buffer or integer>
   and :ex-handler <an error handling function or arity 1> which override the defaults. See
   tansducer macro in this file. Also see core.async documentation for chan:
   https://clojure.github.io/core.async/index.html#clojure.core.async/chan"
   
  [g & {:keys [buffer]
          :or {buffer default-buffer}}]
  (if-let [norm-g (normalize g)]
    (->AsyncGraph norm-g (graph-channels norm-g buffer) buffer)
    (throw (Exception. "the directed graph can't have cycles."))))

(defn- async-graph-intl
  "Private function for constructing an AsyncGraph directly."
  [g ch buf]
  (if (normalize g)
    (->AsyncGraph g ch buf)
    (throw (Exception. "the directed graph can't have cycles."))))

(defn- in-chan
  "Returns the in channel of the specified node in chls, a map of node
   to [in-channel out-channel"
  [chls node]
  (if-let [io (get chls node)]
    (:channel io)
    nil))

(defn- out-chan
  "Returns the out channel (a mult) of the specified node in chls, a map
   of node to [in-channel out-channel]."
  [chls node]
  (if-let [io (get chls node)]
    (:mult io)
    nil))

(defn- tap-into
  "Taps the node's channel into the out-channels of each of the nodes"
  [chls nodes node]
  (doseq [p nodes] (tap (out-chan chls p) (in-chan chls node))))

(defn- untap-from
  "Untaps the node's channel from the out-channels of each of the nodes"
  [chls nodes node]
  (doseq [p nodes] (untap (out-chan chls p) (in-chan chls node))))

(defn- graph-add-node
  "Creates a core.async channel with the transducer xf and buffer buf applied.
   Asssociates the new 'node' into g, the existing graph of transducers and ch, the
   existing map of transducers to their in and out channels."
  [g ch xf children buf]
  (let [g'  (assoc g xf children)
        in  (make-chan buf xf)
        io  {:channel in :mult (if (empty? children) nil (mult in))}
        ch' (assoc ch xf io)]
    [g' ch']))
;; TODO - these needs to be recursive checking for leaves etc.
;; Maybe just rationalise with assoc


(defn- graph-assoc
  "Associates k (a transducer) v (its child transducers) into the graph specified
   in g (graph of transducers) and ch (map of transducer to [in-chan out-chan].
   Recursively merges any children specified in v into the graph. If any children
   in v exist in the graph already, will continue to recurse re-adding children until
   leaf nodes are struck.
   Effectively branches the graph at the point of insertion using a persistent
   data structure structural sharing approach: nodes and their channels upstream
   of the inserted node are not touched but downstream nodes are effectively branched
   off with new channels from that point.
   Returns a 2-vector of new graph of transducers, new map of channels - i.e. the new
   data structure."
  ;; Note: consider doing a transient version of this operation for large graphs.
  [g ch k v buf]
  (let [[g' ch'] (graph-add-node g ch k v buf)] ;; add node to existing g & ch
    ;; tap the new node into its parents
    (tap-into ch' (parents g' k) k)
    (if (empty? v)
      [g' ch']
      (reduce
          (fn [[g1 ch1] cur]
            (map merge [g1 ch1]
                 (graph-assoc g1 ch1 cur (union #{} (get g' cur)) buf)))
          [g' ch']
          v))))

(defn- graph-dissoc
  "Dissociates k from the graph. Fully orphaned children of k are also dissoc'd.
  Any parents of k that become leaves in the graph are recreated without a mult channel.
   Returns a 2-vector of new graphof transducers, new map of channels - i.e. the new data structure.
   Note: assoc'ing a node and then dissoc'ing it usually won't result in the original graph!"
  [g ch xf buf]
  (let [prnts (parents g xf)
        ;; remove xf from the children of each of its parents &
        ;; dissoc xf from the graph
        g' (-> (merge g (into {} (map (fn [p] [p (disj (get g p) xf)]) prnts)))
               (dissoc xf))
        ;; find the parents which are now leaves
        prnts-now-leaves (intersection (set prnts) (leaves g'))
        ;; find the parents which are not leaves
        prnts-ok (intersection (set prnts) (non-leaves g'))
        ;; make new un-multed channels for those parents now leaves &
        ;; dissoc xf from channels
        ch' (-> (merge ch
                       (into {}
                             (map (fn [p] [p {:channel (make-chan buf p) :mult nil}])
                                  prnts-now-leaves)))
                (dissoc xf))
        ;; find the children of the node with no other parents
        ;; (some children of xf might also be children of remaining nodes)
        children-g' (apply union (vals g'))
        orphans (filter #(empty? (intersection children-g' #{%})) (get g xf))]
    [(apply dissoc g' orphans) (apply dissoc ch' orphans)]))


(defmacro transducer
  "A convenience macro for defining a transducer function. the-name is the
   name of the var bound to the transducer function, xform. props is a series
   of key values which are copied into the metadata of the function.
   :buf-or-n and ex-handler props are used in the construction of the core.async
   channel when an AsyncGraph is created or assoc'd into.
  
   Ex. Usage: (transducer f1 (map inc) :buf-or-n 15 :ex-handler (fn [n] n))
              (assoc my-g f1 #{f2 f3})
   Note: Not to be used nested inside an assoc."
  [the-name xform & props]
  `(def ~the-name (with-meta ~xform ~(into {} (map vec (partition 2 props))))))
