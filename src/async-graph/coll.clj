(ns async-graph.coll)

(defn merge!
  "An implementation of `merge` using transients.
   xmap is a transient. ymap doesn't have to be."
  [xmap ymap]
  (reduce
   (fn [res [k v]] (assoc! res k v))
   xmap
   ymap))

(defn concat!
  "An example implementation of `concat` for transients.
   xs is a transient. ys doesn't need to be."
  [xs ys]
  (reduce
   (fn [acc cur] (conj! acc cur))
   xs
   ys))
