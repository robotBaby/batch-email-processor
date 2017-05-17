(ns emails.core-test
  (:require [clojure.test :refer :all]
            [emails.core :refer :all]))

(deftest check_lazy
	(is (instance? clojure.lang.LazySeq (ask_Randy 10000000))))

(deftest check_duplicates
	(let [all_records [{:email-address "ab.com" :spam-score 0.5}, 
						{:email-address "ab.com" :spam-score 0.25}, 
						{:email-address "ab.com" :spam-score 0.03},
						{:email-address "cd.com" :spam-score 0.01}]
		duplicate_list (dups-with-function all_records :email-address)
		duplicates (reduce create_duplicate_map {} duplicate_list)
		result (reduce curate_emails {:coll [] :seen #{} :mean 0 :duplicates duplicates :N 0 :processed 0 :last_hundred []} all_records)]
	(is (= duplicates {"ab.com" 0.03}))
	(is (= (:seen result) #{"ab.com"}))
	(is (= (:coll result) [{:email-address "ab.com" :spam-score 0.03}, {:email-address "cd.com" :spam-score 0.01}]))))
