name := "forklift-server"

version := "0.1"

scalaVersion := "2.11.4"

enablePlugins(JavaAppPackaging)

javacOptions ++= Seq("-source", "1.8")

initialize := {
  val _ = initialize.value
  if (sys.props("java.specification.version") != "1.8")
    sys.error("Java 8 is required for this project.")
}

libraryDependencies ++= Seq(
  "forklift" % "forklift" % "0.1",
  "forklift-activemq" % "forklift-activemq" % "0.1",
  "org.springframework" % "spring-jms" % "4.1.1.RELEASE",
  "ch.qos.logback" % "logback-classic" % "1.0.13",
  "org.apache.geronimo.specs" % "geronimo-jms_1.1_spec" % "1.1.1",
  "org.apache.activemq" % "activemq-all" % "5.8.0",
  "org.reflections" % "reflections" % "0.9.9-RC1",
  "commons-io" % "commons-io" % "2.4" % "test",
  "junit" % "junit" % "4.11"  % "test",
  "com.novocode" % "junit-interface" % "0.10" % "test",
  "ch.qos.logback.contrib" % "logback-json-core" % "0.1.2" % "runtime",
  "ch.qos.logback.contrib" % "logback-json-classic" % "0.1.2" % "runtime",
  "ch.qos.logback.contrib" % "logback-jackson" % "0.1.2" % "runtime",
  "org.codehaus.janino" % "janino" % "2.6.1" % "runtime"
)
