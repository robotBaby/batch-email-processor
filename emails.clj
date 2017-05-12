(defn rand-string [characters n]
  (->> (fn [] (rand-nth characters))
       repeatedly
       (take n)
       (apply str)))

(def email-regex
  #"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,63}$")
(def email-domains ["linkedarkpattern.com" "lice.com" "dired.com" "careershiller.com" "glassbore.com" "indeediot.com" "monstrous.com"])

(defn generate-email-record 
	[email-domains _]
	;;;; Setting the max length of email to 7 arbitrarily for generating collision
	(let [n (+ (rand-int 7))
		  ;;; Getting rid of -%+- because I have never seen them in emails
		  characters (lazy-seq "0123456789abcdefghijklmnopqrstuvwxyz.")
		  domain (rand-nth email-domains)
		  temp-email (str (rand-string characters n) "@" domain)
		  score (rand 1)]
    (if (and (> n 0) (re-matches email-regex temp-email))
    	{:email-address temp-email :spam-score score}
    	)))

(defn ask_Randy
	[n] 
	(filter #(not (nil? %)) (map #(generate-email-record email-domains %) (range n))))

(defn dups-with-function
  [seq f]
  (->> seq
       (group-by f)
       ; filter out map entries where its value has only 1 item 
       (remove #(= 1 (count (val %))))))


(defn create_duplicate_map
	[coll duplicate]
	(let [min_spam-score (:spam-score (apply min-key (fn [x] (:spam-score x)) (nth duplicate 1)))
		email-address (nth duplicate 0)]
		(assoc coll (nth duplicate 0) min_spam-score)))

(instance? clojure.lang.LazySeq (ask_Randy 10000))



(defn curate_emails
	[{coll :coll mean :mean N :N :as old} record]
	(let [email-address (:email-address record)
		  spam-score (:spam-score record)
		  new-mean (float (/ (+ spam-score (* mean N)) (inc N)))]
		  ;; The multiple if statements are meant to 
		  ;; speed up the computation by dropping records that fail the criteria 
		  (if (and (< spam-score 0.3) (< new-mean 0.05))
		  	(let [last_hundred (map #(:spam-score %) (conj (take-last 99 coll) record))
		  		  sum_last_hundred (reduce + last_hundred)
		  		  last_hundred_mean (float (/ sum_last_hundred (count last_hundred)))]
		  		(if (< last_hundred_mean 0.1)
				  	(let [all_addresses (map #(:email-address %) coll)
				  		  address_absent (not (some #{email-address} all_addresses))]
				  		  (if (= true address_absent)
				  		  {:coll (conj coll record) :mean new-mean :N (inc N)}
				  		  old))
		  	old))
		  old)))



;;(time (count (:coll (reduce curate_emails {:coll [] :mean 0 :N 0} (ask_Randy 1000000)))))
;;"Elapsed time: 351561.511582 msecs"

(defn curate_emails
	[{coll :coll mean :mean seen :seen duplicates :duplicates N :N processed :processed :as old} record]
	(let [email-address (:email-address record)
		  spam-score (:spam-score record)
		  new-mean (float (/ (+ spam-score (* mean N)) (inc N)))]
		  ;; The multiple if statements are meant to 
		  ;; speed up the computation by dropping records that fail initial criteria 
		  (if (and (< spam-score 0.3) (< new-mean 0.05))
		  	(let [last_hundred (map #(:spam-score %) (conj (take-last 99 coll) record))
		  		  sum_last_hundred (reduce + last_hundred)
		  		  last_hundred_mean (float (/ sum_last_hundred (count last_hundred)))]
		  		(if (< last_hundred_mean 0.1)
		  			(if (contains? duplicates email-address)
		  				(if (not (contains? seen email-address))
						  	(let [new_record (assoc record :spam-score (get duplicates email-address))]
						  		  (if (= 0 (rem N 10000))
		  							(println "Processed ", processed, "email & Added ", N, " emails at ", (System/currentTimeMillis))
		  							;;(print "Last 100 mean: ", last_hundred_mean, "Current mean: ", new-mean, "\n")
		  							)
						  		  {:coll (conj coll new_record) :mean new-mean :seen (conj seen email-address) :duplicates duplicates :N (inc N) :processed (inc processed)})
						  	(assoc old :processed (inc processed)))
		  				(do 
		  					(if (= 0 (rem N 10000))
		  						(println "Processed ", processed, "email & Added ", N, " emails at ", (System/currentTimeMillis))
		  						;;(print "Last 100 mean: ", last_hundred_mean, "Current mean: ", new-mean, "\n")
		  						)
					  		{:coll (conj coll record) :mean new-mean :seen seen :duplicates duplicates :N (inc N) :processed (inc processed)}))
		  	(assoc old :processed (inc processed))))
		  (assoc old :processed (inc processed)))))


(defn curate_emails
	[{coll :coll 
		mean :mean 
		seen :seen 
		duplicates :duplicates 
		N :N 
		processed :processed 
		last_hundred :last_hundred :as old} record]
	(let [email-address (:email-address record)
		  spam-score (:spam-score record)
		  new-mean (float (/ (+ spam-score (* mean N)) (inc N)))]
		  ;; The multiple if statements are meant to 
		  ;; speed up the computation by dropping records that fail initial criteria 
		  (if (and (< spam-score 0.3) (< new-mean 0.05))
		  	(let [new_last_hundred (if (< (count last_hundred) 100) 
		  							(conj last_hundred record) 
		  							(conj (rest last_hundred) record)) ;; dropping the oldest of the 100 record window is full
		  		  sum_last_hundred (reduce + (map :spam-score new_last_hundred))
		  		  last_hundred_mean (float (/ sum_last_hundred (count new_last_hundred)))]
		  		(if (< last_hundred_mean 0.1)
		  			;; If we don't know this is a duplicate, we add it if the previous conditions passed
		  			(if (contains? duplicates email-address)
		  				;; We we know that this is one of the duplicates, check if it has been added before
		  				(if (not (contains? seen email-address))
		  					;; If not seen before add the record with the lowest spam score.
		  					;; This strategy helps us send more email
						  	(let [new_record (assoc record :spam-score (get duplicates email-address))]
						  		  ;; Printing out some fun statistics for every 10000 record
						  		  (if (= 0 (rem N 10000))
						  		  	(do 
			  							(println "Processed ", processed, "email & Added ", N, " emails at ", (System/currentTimeMillis))
			  							(print "Last 100 mean: ", last_hundred_mean, "Current mean: ", new-mean, "\n")))
						  		  {:coll (conj coll new_record) 
						  		  	:mean new-mean 
						  		  	:seen (conj seen email-address) 
						  		  	:duplicates duplicates 
						  		  	:N (inc N) 
						  		  	:processed (inc processed)
						  		  	:last_hundred new_last_hundred})
						  	(assoc old :processed (inc processed)))
		  				(do 
		  					(if (= 0 (rem N 10000))
						  		  ;; Printing out some fun statistics for every 10000 record
		  						(do 
			  						(println "Processed ", processed, "email & Added ", N, " emails at ", (System/currentTimeMillis))
			  						(print "Last 100 mean: ", last_hundred_mean, "Current mean: ", new-mean, "\n")))
		  					;; If not a duplicate, add it away!
					  		{:coll (conj coll record) 
					  			:mean new-mean 
					  			:seen seen 
					  			:duplicates duplicates 
					  			:N (inc N) 
					  			:processed (inc processed)
					  			:last_hundred new_last_hundred}))
		  	(assoc old :processed (inc processed))))
		  (assoc old :processed (inc processed)))))

(defn get_batch_record
	[{coll :coll mean :mean duplicates :duplicates seen :seen :as old} record]
	(let [email-address (:email-address record)
		  spam-score (:spam-score record)
		  N (count coll)
		  new-mean (float (/ (+ spam-score (* mean N)) (inc N)))]

		  (if (and (< spam-score 0.3) (< new-mean 0.05))
		  	(let [last_hundred (map #(:spam-score %) (conj (take-last 99 coll) record))
		  		  sum_last_hundred (reduce + last_hundred)
		  		  last_hundred_mean (float (/ sum_last_hundred (count last_hundred)))]
		  		(if (< last_hundred_mean 0.1)

		  			;;(if (contains? duplicates email-address)
		  				;;(if (not (contains? seen email-address))
		  					(let [all_addresses (map #(:email-address %) coll)
				  		  		  address_absent (not (some #{email-address} all_addresses))
				  		  		  new_record (assoc record :spam-score (get duplicates email-address))]
							  	  (if (= true address_absent)
							  		  {:coll (conj coll new_record) :mean new-mean :duplicates duplicates :seen (conj seen email-address)}
							  		  old))
		  				;;	old)
		  			;;{:coll (conj coll record) :mean new-mean  :duplicates duplicates :seen (conj seen email-address)})
		  			old))
		  	old)))


(defn parallel_curate
	[all_records]
	(let [N (count all_records)
		 chunk_size (quot N 1000)
		 chunks (partition-all chunk_size all_records)
		 first_pass (map #(:coll (reduce curate_emails {:coll [] :mean 0 :N 0} %)) chunks)
		 _ (println "Done with first pass")
		 duplicates (map #(nth % 0) (dups-with-function (flatten first_pass) :email-address))
		 _ (println duplicates)
		 final_pass (reduce curate_emails {:coll [] :mean 0 :N 0 :duplicates duplicates} first_pass)]
	(count (:coll final_pass))))


;;(time (count (:coll (reduce curate_emails {:coll [] :mean 0 :N 0} (ask_Randy 1000000)))))
;;"Elapsed time: 351561.511582 msecs"
;;(def a (take 100 (repeatedly #(ask_Randy 10000))))
;;(time (map #(count (:coll (reduce curate_emails {:coll [] :mean 0 :N 0} %))) a))
;;(mapcat #(:coll (reduce curate_emails {:coll [] :mean 0 :N 0} %)) a)
;;(time (def result (reduce curate_emails {:coll [] :seen [] :mean 0 :duplicates duplicates :N 0 :processed 0 :last_hundred []} all_records)))


(def all_records (ask_Randy 5000000))

(def duplicates (reduce create_duplicate_map {} (dups-with-function all_records :email-address)))











