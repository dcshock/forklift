name := "forklift-activemq"

libraryDependencies ++= Seq(
  "org.apache.activemq" % "activemq-client" % "5.14.0",
  "org.apache.activemq" % "activemq-broker" % "5.14.0",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.7.3",
  "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % "2.7.3",
  "org.apache.geronimo.specs" % "geronimo-jms_1.1_spec" % "1.1.1"
)

lazy val testDependencies = Seq(
  "commons-io" % "commons-io" % "2.4",
  "com.novocode" % "junit-interface" % "0.11",
  "org.apache.activemq" % "activemq-all" % "5.14.0"
)

libraryDependencies ++= testDependencies.map(_ % "test")

// Integration tests use embedded servers.  We can only allow one instance at a time
// so disable parallel test execution
parallelExecution in Test := false
testOptions += Tests.Argument(TestFrameworks.JUnit, "-v", "-a")
