(defproject backtype/cascading-dbmigrate "1.0.1"
  :java-source-path "src/jvm"
  :java-fork "true"
  :javac-debug "true"
  :hooks [leiningen.hooks.javac]
  :dependencies [[cascading1.1 "1.1.3-SNAPSHOT"]
                 ]
  :repositories {"conjars" "http://conjars.org/repo/"}
  :dev-dependencies [[org.apache.hadoop/hadoop-core "0.20.2-dev"]
                     [lein-javac "1.2.1-SNAPSHOT"]
                    ])
