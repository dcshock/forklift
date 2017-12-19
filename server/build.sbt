name := "forklift-server"

enablePlugins(JavaAppPackaging)

libraryDependencies ++= Seq(
  "com.github.dcshock" % "consul-rest-client" % "0.10",
  "org.apache.activemq" % "activemq-broker" % "5.14.0",
  "io.searchbox" % "jest" % "2.0.0",
  "args4j" % "args4j" % "2.0.31",
  "org.codehaus.janino" % "janino" % "2.6.1",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.7.3",
  "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % "2.7.3",
  "ch.qos.logback.contrib" % "logback-json-core"    % "0.1.2",
  "ch.qos.logback.contrib" % "logback-json-classic" % "0.1.2",
  "ch.qos.logback.contrib" % "logback-jackson"      % "0.1.2",
  "javax.inject" % "javax.inject" % "1",
  "com.novocode" % "junit-interface" % "0.11" % "test",
  "commons-io" % "commons-io" % "2.4" % "test"
)
