(ns async-graph.core2
  (:require [clojure.core.async :refer
             [>! <! >!! <!! go chan buffer close! thread alts! alts!!
              timeout mult tap untap pipeline onto-chan]]
            [async-graph.graph2 :refer
             [normalize kahn-sorted roots edges leaves not-leaves nodes
              parents-of parent? root? leaf? not-roots]]
            [clojure.set :refer [difference union intersection]]
            [clojure.core.match :refer [match]]
            [clojure.spec :as s]
            [clojure.core.async.impl.protocols :as async-protocols]))

(use 'async-graph.coll)

(def default-buffer 10)


;; Some test data during the dev process
(def f1 (map inc))
(def f2 (map dec))
(def f3 (map inc))
(def f4 (map inc))
(def f5 (map dec))

(def nd1 [f1 {:chdn #{f2 f3} :buf-or-n 12 :ex-handler identity :label :inc}])
(def nd2 [f2 {:chdn #{} :buf-or-n 20 :ex-handler identity :label :dec}])
(def defs {::buf-or-n default-buffer})
(def g (into {} [nd1 nd2]))

;; f1 -> f2, f3
;; f2 -> {}
;; f3 -> {}
;; end test data

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

(s/def ::buf-or-n (s/or :buffer #(instance? async-protocols/Buffer %)
                        :n integer?))

(s/def ::chdn (s/and ::set (s/* ::function) (s/* ::arity-1)))

(s/def ::ex-handler arity-1?)

(s/def ::graph (s/keys :req [::chdn]
                       :opt [::buf-or-n ::ex-handler]))


(defn make-chan
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

(defn enable-node
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

(defn is-enabled? [props]
  (if (:in props) true false))

(defn is-leaf? [props]
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

(defn add-new-node-to-its-parents
  "Updates all parent's of xf in g to have xf as one of their children."
  [g xf props]
  (update-in g (vec (parents-of g xf)) link-parent xf props))

(defn remove-node-from-parents
  "Removes child relationship from all parents of xf in the graph."
  [g xf props]
  (update-in g (vec (parents-of g xf)) unlink-parent xf props))

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

(deftype AsyncGraph [graph default-buffer default-ex-handler])



(defn async-graph
  ""
  [g & {:keys [buf-or-n ex-handler]
        :or {:buf-or-n default-buffer}}]
  (let [defaults (if ex-handler
                   {:buf-or-n buf-or-n :ex-handler ex-handler}
                   {:buf-or-n buf-or-n})
        g-norm (normalize g)]
    (if true ;; (s/valid? ::check/graph g-norm)
      (->AsyncGraph (async-graph-impl g-norm defaults) buf-or-n ex-handler)
      (throw (ex-info "Invalid input" (s/explain-data ::check/graph g-norm))))))
;; here's where I got to


(defn graph-assoc
  ""
  [g k v default-props]
  )

(defn graph-assoc-inner
  ""
  ;; Note: consider doing a transient version of this operation for large graphs.
  [g k v default-props]
  (let [props (if (set? v) (merge default-props {:chdn v}) (merge default-props v))
        g' (assoc g k props)]
    (println props)
    (map #(tap (:out %) (:in props)) (parents-of g' k))
    (if (empty? (:chdn props))
      g'
      (reduce
       (fn [acc cur]
         (map merge g'
              (graph-assoc g' cur
                           (if-let [props' (get g' cur)]
                             props'
                             (enable-node cur {:chdn #{}} default-props :leaf? true)))))
       g'
       (:chdn props)))))


;; algo for assoc k,v:
;; 1. add node.
;; 2. tap into parents
;; 3. for each of children of node:
;;     (re)assoc into graph.
;;     if it's a reassoc, use existing properties. dissoc diff children
;;     if it's a new assoc, default properties from the graph
;;     stop when leaf
;; additional:
;; if v is a set rather than a map and conforms to set of transducers, then
;; make a props with default props around the chldn.
