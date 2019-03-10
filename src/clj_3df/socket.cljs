(ns clj-3df.socket
  "Wrapping JS WebSockets in ClojureScript in an attempt to align the
  interfaces on both platforms.")

(defn connect!
  "Create a WebSocket to the specified URL"
  [url on-open on-message on-close]
  (let [socket (js/WebSocket. url)]
    (set! (.-binaryType socket) "arraybuffer")
    (set! (.-onopen socket)     on-open)
    (set! (.-onmessage socket)  on-message)
    (set! (.-onclose socket)    on-close)
    socket))

(defn close!
  "Close a stream opened by connect."
  [socket]
  (.close socket))

(defn put!
  "Sends a message along this socket."
  ([socket msg] (put! socket msg 1))
  ([socket msg attempt]
   (let [state (.-readyState socket)]
     (when (> attempt 10)
       (throw (ex-info "put! failed" {:socket socket :msg msg :attempt attempt})))
     (cond
       (= state js/WebSocket.CONNECTING) (js/setTimeout #(put! socket msg (inc attempt)) (* attempt 100))
       (= state js/WebSocket.OPEN)       (.send socket msg)
       (= state js/WebSocket.CLOSED)     (throw (ex-info "Attempted put! on a closed socket" {:socket socket}))
       (= state js/WebSocket.CLOSING)    (throw (ex-info "Attempted put! on a closing socket" {:socket socket}))
       :else                             (js/setTimeout #(put! socket msg (inc attempt)) (* attempt 100))))))
