(ns async-graph.core2
  (:require [clojure.core.async :refer
             [>! <! >!! <!! go chan buffer close! thread alts! alts!!
              timeout mult tap untap pipeline onto-chan]]
            [async-graph.graph2 :refer
             [normalize kahn-sorted roots edges leaves not-leaves nodes
              parents-of parent? root? leaf? not-roots bfs-multi scc]]
            [clojure.set :refer [difference union intersection]]
            [clojure.core.match :refer [match]]
            [clojure.spec :as s]
            [clojure.core.async.impl.protocols :as async-protocols]
            [potemkin :refer [def-map-type]]))


(def default-buffer 10)


;; Some test data during the dev process
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

(def g11 
  {f1 {::chdn #{f3 f4}}
   f2 {::chdn #{f4 f5}}
   f3 {::chdn #{f6 f7}}
   f4 {::chdn #{f7 f8 f9}}
   f5 {::chdn #{f9 f10}}})

;; end test data

;; Spec for a graph
(defn arity
 "Returns the maximum parameter count of each invoke method found by refletion
  on the input instance. The returned value can be then interpreted as the arity
  of the input function. The count does NOT detect variadic functions."
  [f]
  (let [invokes (filter #(= "invoke" (.getName %1)) (.getDeclaredMethods (class f)))]
    (apply max (map #(alength (.getParameterTypes %1)) invokes))))

(defn arity-1? [f] (= 1 (arity f)))

;; Spec for a graph of transducers
(s/def ::set set?)

(s/def ::function #(instance? clojure.lang.IFn %))

(s/def ::arity-1 arity-1?)

(s/def ::buf-or-n (s/or :buffer #(satisfies? async-protocols/Buffer %)
                        :n integer?))

(s/def ::empty-set (s/and ::set empty?))
(s/def ::set-of-fns (s/and ::set (s/+ ::function)))

(s/def ::chdn (s/or :empty ::empty-set
                    :children ::set-of-fns))

(s/def ::ex-handler arity-1?)

(s/def ::graph-props (s/keys :req [::chdn]
                             :opt [::buf-or-n ::ex-handler]))

(s/def ::graph (s/map-of ::function ::graph-props))
;; End of spec section

(defn ^:private make-chan
  "Makes a core.async channel with/out transducer xf, trying to obtain buffer
   and exception handler from the supplied node properties map."
  ([props]
   (match [props]
          [{:buf-or-n b}] (chan b)
          :else (chan)))
  ([xf props]
   (match [props]
          [{:buf-or-n b :ex-handler e}] (chan b xf e)
          [{:buf-or-n b}] (chan b xf)
          :else (throw (Exception. "need at least buf-or-n to form transducer channel.")))))

(defn ^:private enable-node
  "Creates the :in and :out channels for the node, making a pipeline :parallelism n
   is present, otherwise a single channel if the node is a leaf or a chan-mult pair
   if not. The transducer xf is applied either over the pipeline or channel."
  [xf props & {:keys [leaf?] :or {leaf? false}}]
  (match [props]
         [{:parallelism n :ex-handler e}]
         (let [in (make-chan props)
               out (make-chan props)]
           (pipeline n out xf in e)
           (merge props (if leaf? {:in in :out out}
                            {:in in :out out :mult (mult out)})))
         
         [{:parallelism n}]
         (let [in (make-chan props)
               out (make-chan props)]
           (pipeline n out xf in)
           (merge props (if leaf? {:in in :out out}
                            {:in in :out out :mult (mult out)})))

         :else ;;Not a pipeline
         (if leaf?
           (merge props (let [c (make-chan props)]
                          {:in c :out c :mult nil}))
           (merge props (let [c (make-chan props)]
                          {:in c :out c :mult (mult c)})))))

(defn ^:private is-enabled? [props]
  (if (:in props) true false))

(defn ^:private is-leaf? [props]
  (if (:mult props) false true))

;; Leave these TWO for now and see if I need them.
(defn link-parent
  "Adds a child [xf props] to specified parent's props, adding a :mult
   to the parent if previously a leaf. Returns the updated parent's props.
   Assumes that the parent node is already enabled."
  [parent-props xf props]
  (if (nil? parent-props)
    parent-props
    (let [pp' (if (is-leaf? parent-props)
                (let [c (:out parent-props)]
                  (merge parent-props {:mult (mult c)}))
                parent-props)]
      (tap (:out pp') (:in props))
      (update pp' :chdn conj xf))))

(defn unlink-parent
  "Untaps and removes the child xf from parent-props, converting it to a
   leaf (no :mult) if xf was the only child."
  [parent-props xf props]
  (if (nil? parent-props)
    parent-props
    ((untap (:mult parent-props) (:in props))
     (let [pp' (update-in parent-props [:chdn] disj xf)]
       (if (empty? (:chdn pp'))
         (merge pp' {:mult nil}) ;; parent now a leaf
         pp')))))

(defn ^:private add-new-node-to-its-parents
  "Updates all parent's of xf in g to have xf as one of their children."
  [g xf props]
  (update-in g (vec (parents-of g xf)) link-parent xf props))

(defn ^:private remove-node-from-parents
  "Removes child relationship from all parents of xf in the graph."
  [g xf props]
  (update-in g (vec (parents-of g xf)) unlink-parent xf props))

(defn ^:private recreate-nodes
  "Creates a seq of [node node-properties] of the specified set of nodes in the graph.
   The :in, :out and :mult channels in node-properties are new."
  [g nodes]
  (map #(vector % (enable-node % (g %))) nodes))

(defn ^:private connect-nodes
  "Connections each node into its parents in the graph."
  [g nodes]
  (doseq [n nodes]
      (map #(tap (:mult (g %)) (:in (g n))) (parents-of g n))))

(defn ^:private graph-assoc
  "Associates a node into the graph, merging default-props into v.
   The existing graph is branched at the point of insertion with k and all child
   nodes of k being recreated, maximising sharing of nodes."
  [g k v default-props]
  (let [affected (bfs-multi g (:chdn v))
        ops (into {} (cons [k (merge default-props v)] (recreate-nodes g affected)))
        g' (merge g ops)]
    (connect-nodes g (reverse affected))
    g'))

(defn ^:private graph-dissoc
  "Dissociates a node into the graph.
   The existing graph is branched at the point of dissocation with all child
   nodes of k being recreated, maximising sharing of nodes."
  [g k]
  (let [affected (bfs-multi g (:chdn (g k)))
        ops (into {} (recreate-nodes g affected))
        g' (merge g ops)]
    (connect-nodes g (reverse affected))
    g'))

(def-map-type AsyncGraph [graph default-buffer default-ex-handler mta]
  (get [_ k _]
       (graph k))
  (assoc [_ k v]
         (graph-assoc graph k v {:buf-or-n default-buffer :ex-handler default-ex-handler} mta))
  (dissoc [_ k]
         (graph-dissoc graph k {:buf-or-n default-buffer :ex-handler default-ex-handler} mta))
  (keys [_]
        (keys graph))
  (meta [_]
        mta)
  (with-meta [_ mta]
    (AsyncGraph. graph default-buffer default-ex-handler mta)))

(defn ^:private async-graph-impl
  "Makes an async-graph from supplied g (graph) and map of defaults
   expected to contain :buf-or-n and :buf-or-n & :ex-handler keys"
  [g defaults]
  (let [graph (into {}
                    (map (fn [[k v]]
                           [k (if (set? v)
                                (enable-node k (merge defaults {:chdn v})
                                             :leaf? (leaf? g k))
                                (enable-node k (merge defaults v)
                                             :leaf? (leaf? g k)))])
                         g))]
    (doseq [e (edges g)] (tap (:mult (get graph (first e)))
                              (:in (get graph (second e)))))        
    graph))

(defn async-graph
  ""
  [g & {:keys [buf-or-n ex-handler]
        :or {buf-or-n default-buffer}}]
  
  (let [defaults (if ex-handler
                   {:buf-or-n buf-or-n :ex-handler ex-handler}
                   {:buf-or-n buf-or-n})
        g-norm (normalize g)]
    (if (and
         (s/valid? ::graph g)
         (if ex-handler (s/valid? ::ex-handler ex-handler) true)
         (s/valid? ::buf-or-n buf-or-n))
      1 ;(->AsyncGraph (async-graph-impl g-norm defaults) buf-or-n ex-handler)
      (throw (ex-info "Invalid input" )))))
