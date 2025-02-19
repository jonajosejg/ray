(set-env!
  :resource-paths #{"src/common"}
  :dependencies '[[tailrecursion/boot-jetty                 "0.1.3"           :scope "test"]
                  [adzerk/boot-beanstalk                    "0.7.3"           :scope "test"]
                  [clj-http                                 "2.3.0"           
                                                                              :exclusions [org.apache.httpcomponents/httpclient
                                                                                           org.apache.httpcomponents/httpcore
                                                                                           commons-io]]
                  [com.climate/squeedo                      "0.1.4" :exclusions [joda-time]]
                  [org.clojure/core.async                   "0.3.443"]
                  [org.clojure/clojure                      "1.8.0"]
                  [abengoa/clj-stripe                       "1.0.4" :exclusions  [org.clojure/clojure org.clojure/data.json
                                                                                  clj-http]]
;                 [rowtr/clj-stripe                         "1.0.0-SNAPSHOT"]
;                 [io.aviso/pretty                          "0.1.33"]
                  [org.martinklepsch/s3-beam                "0.6.0-alpha3" :exclusions [org.clojure/data.json]]
                  [ring                                     "1.5.0"]
                  [selmer                                   "1.10.6"]
;                 [com.climate/claypoole                    "1.1.4"]
;                 [joda-time                                "2.9.9"]
                  [medley                                   "0.8.4"]
;                 [com.taoensso/nippy                       "2.12.1"]
;                 [com.taoensso/encore                      "2.88.2"]
                  [hoplon/castra                            "3.0.0-alpha7"]
                  [amazonica                                "0.3.71" :exclusions [com.taoensso/encore com.taoensso/nippy joda-time]]
;                 [com.cemerick/bandalore                   "0.0.6"]
                  [dk.ative/docjure                         "1.10.0"]
                  [com.taoensso/carmine                     "2.16.0"]
;                 [com.fasterxml.jackson.core/jackson-core  "2.6.6"]
                  [camel-snake-kebab                        "0.4.0"]
                  [com.papertrailapp/logback-syslog4j       "1.0.0"]
;                 [slingshot                                "0.12.2"]
                  [org.clojure/java.jdbc                    "0.4.2"]
                  [org.postgresql/postgresql                "42.2.5"]
                  [clj-postgresql                           "0.7.0"]
                  [rowtr/form                               "0.2.5"]
                  [rowtr/geo-cache                          "1.0.0-SNAPSHOT" :exclusions [org.slf4j/slf4j-nop]]
                  [rowtr/geo-graph                          "1.0.0-SNAPSHOT"]
                  [org.clojure/tools.logging                "0.3.1"]
                  [ch.qos.logback/logback-classic           "1.1.3" ]
                  [tailrecursion/cljson                     "1.0.7"]
;                 [rowtr/s3-beam                            "0.1.0"]
                  [rowtr/google-maps-web-api                "0.1.8"]
                  [jumblerg/ring.middleware.cors            "1.0.1"]
                  [digest                                   "1.4.4"]
                  [ring-basic-authentication                "1.0.6"]
                  [clj-time                                 "0.13.0"]
                  [factual/geo                              "1.0.0"]
                  [adzerk/env                               "0.4.0"]
                  [cheshire                                 "5.7.0"]
                  [tsp                                      "3.0.0-SNAPSHOT"]
                  [clojurewerkz/quartzite                   "2.0.0"]
                  [compojure                                "1.3.4"]
                  [liberator                                "0.14.1"]])

(require
  '[adzerk.env :as env]
  '[tailrecursion.boot-jetty               :refer [serve]]
  '[adzerk.boot-beanstalk :refer :all])

(task-options!
  serve       {:port 8000}
  uber        {:as-jars true}
  beanstalk   {:access-key      (System/getenv "ELB_KEY")
                :secret-key      (System/getenv "ELB_SECRET")
                :version         "1.0.0"
                :stack-name      "64bit Amazon Linux 2018.03 v3.0.3 running Tomcat 8.5 Java 8"})

(env/def
  REDIS_HOST            "localhost"
  REDIS_PORT            "8332"
  CACHE_TYPE            "redis"
  LOG_LEVEL             "INFO"
  ANT_COLONY_TRIALS     "5"
  FILE_IN_QUEUE         "file-in-dev"
  FILE_OUT_QUEUE        "file-out"
  JOB_QUEUE             "dev-tsp-jobs"
  )

(deftask castra
  "task to set castra only for local dev"
  []
  (set-env! :resource-paths #(conj % "src/castra"))
  (task-options!
    beanstalk         #(assoc % :name "castra"
                                :description "Castra API for hoplon webapps"
                                :beanstalk-envs [{:name "castra-production"
                                                  :cname-prefix "castra-production"}])
    web               {:serve 'co.rowtr.castra.core/app})
  (web :serve 'co.rowtr.castra.core/app))

(deftask restful 
  "taks to set liberator+castra for local dev"
  [d dev bool "Development environment"]
  (let [app-name    (if dev "tsp-api-dev" "tsp-api")
        env-name    (if dev "api-dev" "tsp-api")]
    (set-env! :resource-paths #(conj % "src/tsp-api" "src/rest"))
    (task-options!
      beanstalk         #(assoc % :name app-name
                                  :description "REST API for customers"
                                  :beanstalk-envs [{:name         env-name
                                                    :cname-prefix env-name}
                                                    ])
      web               {:serve 'co.rowtr.tsp-api.core/app})))

(deftask worker
  "task to set worker"
  [d dev bool "Development environment"]
  (let [app-name    (if dev "tsp-worker-dev" "tsp-worker")
        env-name    (if dev "tsp-worker-dev" "worker-prod")]
    (println app-name env-name)
    (set-env! :resource-paths #(conj % "src/tsp-api" "src/worker"))
    (task-options!
      beanstalk         #(assoc % :name app-name 
                                  :description "Worker for processing files and TSP jobs"
                                  :beanstalk-envs [{:name        env-name 
                                                    :cname-prefix env-name}])
      web               {:serve           'co.rowtr.tsp-api.core/app
                        :context-create  'co.rowtr.tsp-api.init/init-server
                        :context-destroy 'co.rowtr.tsp-api.init/destroy-server})))

(deftask full
  []
  (set-env! :resource-paths #(conj % "src/castra" "src/tsp-api" "src/rest" "src/worker")))

(deftask dev
  "dev task using boot-http"
  []
  (comp
;   (watch)
    (web)
    (serve)
    (notify :audible true :theme "woodblock")))

(deftask build-war
  "build castra tomcat package"
  []
  (comp
    (web)
    (uber)
    (war)))
