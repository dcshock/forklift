name := "forklift-kafka"

libraryDependencies ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.7.3",
  "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % "2.7.3",
  "org.apache.kafka" % "kafka-clients" % "0.10.1.1-cp1" exclude("org.slf4j","slf4j-log4j12"),
  "io.confluent" % "kafka-avro-serializer" % "3.1.1" exclude("org.slf4j","slf4j-log4j12"),
  "org.apache.avro" % "avro" % "1.8.1"
)

lazy val testDependencies = Seq(
  "commons-io" % "commons-io" % "2.4" ,
  "com.novocode" % "junit-interface" % "0.11",
  "commons-net" % "commons-net" % "3.6",
  "org.mockito"       % "mockito-core"            % "1.9.5",
  "io.confluent" % "kafka-schema-registry" % "3.1.1" exclude("org.slf4j","slf4j-log4j12"),
  "org.apache.zookeeper" % "zookeeper" % "3.4.9" exclude("org.slf4j","slf4j-log4j12"),
  //Added as it looks like the schema registry has a hard coded dependency on a log4j class that the bridge does not help with
  "log4j" % "log4j" % "1.2.14"
)

libraryDependencies ++= testDependencies.map(_ % "test")

// Integration tests use embedded servers.  We can only allow one instance at a time
// so disable parallel test execution
parallelExecution in Test := false
testOptions += Tests.Argument(TestFrameworks.JUnit, "-v", "-a")

// avro settings
(javaSource in avroConfig) := baseDirectory(_/"target/generated-sources").value
(sourceDirectory in avroConfig) := baseDirectory(_/"src/test/resources/schemas").value
