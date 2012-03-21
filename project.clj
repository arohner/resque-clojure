(defproject circleci/resque-clojure "0.2.6"
  :description "Redis based library for asynchronous processing"
  :url "https://github.com/jxa/resque-clojure"
  :dependencies [[org.clojure/data.json "0.1.2"]
                 [clj-time "0.3.4"]
                 [redis.clients/jedis "2.0.0"]
                 [org.clojure/tools.logging "0.2.3"]]
  :dev-dependencies [[org.clojure/clojure "1.3.0"]])

