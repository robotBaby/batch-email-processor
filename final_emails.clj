(ns emails
	(:use [clojure.test]))
(defn rand-string [characters n]
  (->> (fn [] (rand-nth characters))
       repeatedly
       (take n)
       (apply str)))

(def email-regex
  #"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,63}$")
(def email-domains 
	["linkedarkpattern.com" 
	"lice.com" "dired.com" 
	"careershiller.com" 
	"glassbore.com" 
	"indeediot.com" 
	"monstrous.com"])
(defn get-time
	[]
	(.format (java.text.SimpleDateFormat. "MM/dd/yyyy : HH:mm:ss") (new java.util.Date)))

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
    	(generate-email-record email-domains _))))

(defn ask_Randy
	[n] 
	(map #(generate-email-record email-domains %) (range n)))

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

(defn compute-new-last-hundred
	[last_hundred record]
	(let [new-last-hundred (if (< (count last_hundred) 100) 
		  							(conj last_hundred record) 
		  							(conj (rest last_hundred) record)) ;; dropping the oldest of the 100 record window is full
		  		  sum-last-hundred (reduce + (map :spam-score new-last-hundred))
		  		  last-hundred-mean (float (/ sum-last-hundred (count new-last-hundred)))]
	{:new-last-hundred new-last-hundred :last-hundred-mean last-hundred-mean}))


(defn print-statictics
	[N processed new-mean last_hundred_mean]
	(if (= 0 (rem N 10000))
	  ;; Printing out some fun statistics for every 10000 record
	(do 
	;;	(println "Processed ", processed, " Added ", N, " time ", (System/currentTimeMillis))))
		(println "Processed ", processed, "email & Added ", N, " emails at ", (get-time))
		(print "Last 100 mean: ", last_hundred_mean, "Current mean: ", new-mean, "\n"))))

(defn check-duplicates
	"Checks for duplicates when scanning the bacth of emails"
	[{coll :coll seen :seen duplicates :duplicates N :N processed :processed :as old} 
		{email-address :email-address spam-score :spam-score :as record}
		new-mean
		new_last_hundred]
	(if (contains? duplicates email-address)
	;; We we know that this is one of the duplicates, check if it has been added before
	(if (not (contains? seen email-address))
		;; If not seen before add the record with the lowest spam score.
		;; This strategy helps us send more email
  	(let [new_record (assoc record :spam-score (get duplicates email-address))]
  		{:coll (conj coll new_record) :mean new-mean  :seen (conj seen email-address)  :duplicates duplicates :N (inc N)  :processed (inc processed):last_hundred new_last_hundred})
  	(assoc old :processed (inc processed)))	
		{:coll (conj coll record) :mean new-mean :seen seen :duplicates duplicates :N (inc N) :processed (inc processed):last_hundred new_last_hundred}))

(defn curate_emails
	"Function called in a reduce to check for conditions for adding an email to a batch or not.
	TODO: Do preprocessing and batch processing together."
	[{coll :coll mean :mean seen :seen duplicates :duplicates N :N processed :processed last_hundred :last_hundred :as old} {email-address :email-address spam-score :spam-score :as record}]
	(let [new-mean (float (/ (+ spam-score (* mean N)) (inc N)))]
		  ;; The multiple if statements are meant to 
		  ;; speed up the computation by dropping records that fail initial criteria 
		  (if (and (< spam-score 0.3) (< new-mean 0.05))
		  	(let [{last-hundred-mean :last-hundred-mean new-last-hundred :new-last-hundred} (compute-new-last-hundred last_hundred record)]
		  		(if (< last-hundred-mean 0.1)
		  			;; If we don't know this is a duplicate, we add it if the previous conditions passed
		  			(do
		  				(print-statictics N processed new-mean last-hundred-mean)
		  				(check-duplicates old record new-mean new-last-hundred))
		  (assoc old :processed (inc processed))))
		  (assoc old :processed (inc processed)))))



(defn process_email
	"Entry function tha processes the batch and output the emails to send in order.
	And print statistics"
	[N]
	(let [_ (println "Asking Randy for the emails at ", (get-time))
		 all_records (ask_Randy N)
		 _ (println "Waititng for prep work at ", (get-time))
		 _ (println "Getting duplicates at ", (get-time))
		 duplicate_list (dups-with-function all_records :email-address)
		 _ (println "Getting minimum for the duplicates at ", (get-time))
		 duplicates (reduce create_duplicate_map {} duplicate_list)
		 _ (println "Starting the batch processing of email at ", (get-time))
		 _ (Thread/sleep 100)
		 result (reduce curate_emails {:coll [] :seen #{} :mean 0 :duplicates duplicates :N 0 :processed 0 :last_hundred []} all_records)]
	(println "Done Processing at ", (get-time))
	(println "##############")
	(println "Total emails processed: ", (:processed result))
	(println "Total emails added to the batch: ", (:N result))
	(println "Number of duplicate emails in this batch was: ", (count duplicates))
	(println "Number of duplicate emails added to this batch: ", (count (:seen result)))
	(println "Final mean: ", (:mean result))
	(println "##############")))


(deftest check_lazy
	(is (instance? clojure.lang.LazySeq (ask_Randy 10000000)) "Checking lazy"))

(deftest check_duplicates
	(let [all_records [{:email-address "ab.com" :spam-score 0.5}, 
						{:email-address "ab.com" :spam-score 0.25}, 
						{:email-address "ab.com" :spam-score 0.03},
						{:email-address "cd.com" :spam-score 0.01}]
		duplicate_list (dups-with-function all_records :email-address)
		duplicates (reduce create_duplicate_map {} duplicate_list)
		result (reduce curate_emails {:coll [] :seen #{} :mean 0 :duplicates duplicates :N 0 :processed 0 :last_hundred [] :last_hundred_mean 0} all_records)]
	(is (= duplicates {"ab.com" 0.03}) "Checks for suplicate" )
	(is (= (:seen result) #{"ab.com"}) "Checks how the seen bahaves")
	(is (= (:coll result) [{:email-address "ab.com" :spam-score 0.03}, {:email-address "cd.com" :spam-score 0.01}]) "Checks the overall outcome")))

