;; @author Viksit Gaur (http://viksit.com)
;; @copyright (c) 2014 Viksit Gaur
;; @description A simple map reduce framework in Clojure
;; @License Eclipse license (http://www.eclipse.org/legal/epl-v10.html)

;; This is a VERY simple map reduce implementation for learning purposes based off the original
;; Google paper from OSDI 2004. Concepts only.
;; I'm going to try and cover more of the concepts in the paper, including combiners, partitioning, output formats and more depending on time.
;; Paper here: http://research.google.com/archive/mapreduce.html


;; TODO
;; - Basic mapper, reducer, group by, sort and shuffle functionality
;; - Combiner functions
;; - Partitioning
;; - Command line interface for usage
;; - Distributed processes (single node, pseudo mode)
;; - Distributed processes across machines, including job trackers (Probably not getting here any time soon)

(ns learn.mr
  (:require [clojure.string :as str]))

;; ------------------------
;; Utility functions
;; ------------------------

(defn get-key [m k-v]
  "Update a key with a value or a list of values"

  (let [k (first k-v)
        v (last k-v)]
    (if (get m k)
      (do
        (prn "found key in map")
        (update-in m [k]
                   (fn [val]
                     (conj val v))))
      (do
        (prn "key not found" m k v)
        (assoc m k [v])))))

(defn get-val [m k-v]
  "Determine which value to update the given map with"

  (let [k (first k-v)
        v (last k-v)]
    (if (get m k)
      (conj (get m k) v)
      [v])))

;; ------------------------
;; System defined functions
;; ------------------------

(defn apply-mapper-fn [mapper-fn inp]
  "Take a single document and apply a mapper function to it"

  (let [inp-hash (hash inp)
        map-result (mapper-fn inp-hash inp)]
    map-result))

(defn apply-reducer-fn [reducer-fn inp]
  "Take a reducer function and apply it to the input list"
  (map (fn [i]
         (reducer-fn (first i) (last i))) inp))

(defn group-by-keys [key-val-pairs]
  "Take key value pairs and group them"
  (reduce #(assoc %1
             (first %2)
             (get-val %1 %2))
          {}
          key-val-pairs))


(defn -map-reduce [doc-list mapper-fn reducer-fn]
  "Main map reduce flow that we invoke"
  (->>
   ;; Go through all documents
   doc-list
   ;; Perform map task on each
   (mapcat #(apply-mapper-fn mapper-fn %))
   ;; Group these results by the key and sort
   (sort-by first)
   ;; Apply the reduce tasks
   (group-by-keys)
   (apply-reducer-fn reducer-fn)
   ;; Spit the output
   ))



;; --------------------------
;; User defined functions
;; --------------------------

(def *doc-list-full*
  [{:text "the quick brown fox" :url "http://fox.net" :time "2014-04-23T10:13Z"}
   {:text "jumped over" :url "http://jumped.net" :time "2014-04-24T10:13Z"}
   {:text "the lazy dog and" :url "http://dog.net" :time "2014-04-25T10:13Z"}
   {:text "the brown fox was quick" :url "http://fox.net" :time "2014-04-22T10:13Z"}])


(def *doc-list*
  ["the quick brown fox"
   "jumped over"
   "the lazy dog and"
   "the brown fox was quick"])

(def *doc-list-to-index*
  [{:name "fox.txt" :text "the quick brown fox"}
   {:name "jump.txt" :text "jumped over"}
   {:name "lazy.txt" :text "the lazy dog and"}
   {:name "quick.txt" :text "the brown fox was quick"}])

;; --------------------------
;; Word count example
;; --------------------------

;; key is a document id, val is the document text
(defn word-count-mapper [key val]
  ; Implement a mapping function
  (reduce #(assoc %1 %2 1) {} (str/split val #" ")))

(defn word-count-reducer [key values]
  (assoc {} key (apply + values)))

;; --------------------------
;; Url access frequency
;; --------------------------

;; map function processes logs and outputs (url, 1)
(defn url-access-mapper [key val]
  (assoc {} (get val :url) 1))

;; reduce function adds together all the values for the same url
(defn url-access-reducer [key values]
  (assoc {} key (apply + values)))

;; --------------------------
;; Inverted index using map reduce
;; --------------------------

;; map function parses each document and emits (word, docId)
(defn inv-index-mapper [key val]
  (reduce #(assoc %1 %2 (get val :name)) {} (str/split (get val :text) #" ")))

;; reduce function accepts all pairs and emits (word, [docid1, .. n])
(defn inv-index-reducer [key values]
  (assoc {} key values))

;; this is an inverted index


;; Test
;; (-map-reduce *doc-list* word-count-mapper word-count-reducer)
;; (-map-reduce *doc-list-full* url-access-mapper url-access-reducer)
;; (-map-reduce *doc-list-to-index* inv-index-mapper inv-index-reducer)
