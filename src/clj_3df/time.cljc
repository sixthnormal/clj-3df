(ns clj-3df.time
  "Functions for dealing with the various timestamp types supported by
  3DF.")

(defn tx-id
  "Specifies a point within a domain of transaction times or sequence
  numbers."
  [tx]
  {:TxId tx})

(defn real-time
  "Specifies a point within a real-time domain. This can be either a
  wall-clock or a relative duration."
  ([secs] (real-time secs 0))
  ([secs nanos] {:Real {:secs secs :nanos nanos}}))

(defn instant
  "Specifies a point within the domain of unix epochs."
  [timestamp]
  {:TxId timestamp})

(comment

  (tx-id 1)
  
  (real-time 5)
  (real-time 12 123)
  
  )
