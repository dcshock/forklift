organization := "com.github.dcshock"

name := "forklift-kafka"

version := "1.0"

scalaVersion := "2.11.7"

// target and Xlint cause sbt dist to fail
javacOptions ++= Seq("-source", "1.8")//, "-target", "1.8", "-Xlint")

javacOptions in compile ++= Seq("-g:lines,vars,source")

initialize := {
  val _ = initialize.value
  if (sys.props("java.specification.version") != "1.8")
    sys.error("Java 8 is required for this project.")
}

libraryDependencies ++= Seq(
  "com.github.dcshock" % "forklift" % "1.0" ,
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.7.3",
  "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % "2.7.3",
  "org.apache.kafka" % "kafka_2.11" % "0.10.1.0" exclude("org.slf4j","slf4j-log4j12"),
  "io.confluent" % "kafka-avro-serializer" % "3.2.0" exclude("org.slf4j","slf4j-log4j12"),
  "org.apache.avro" % "avro" % "1.8.1" exclude("org.slf4j","slf4j-log4j12")
)

lazy val testDependencies = Seq(
  "commons-io" % "commons-io" % "2.4" ,
  "com.novocode" % "junit-interface" % "0.11",
  "commons-net" % "commons-net" % "3.6",
  "org.mockito"       % "mockito-core"            % "1.9.5",
  "org.apache.zookeeper" % "zookeeper" % "3.4.9" exclude("org.slf4j","slf4j-log4j12"),
  "io.confluent" % "kafka-schema-registry" % "3.1.1" exclude("org.slf4j","slf4j-log4j12"),
  //Added as it looks like the schema registry has a hard coded dependency on a log4j class that the bridge does not help with
  "log4j" % "log4j" % "1.2.14"
)

libraryDependencies ++= testDependencies.map(_ % "test")

// Integration tests use embedded servers.  We can only allow one instance at a time
// so disable parallel test execution
parallelExecution in Test := false

// avro settings
(javaSource in avroConfig) := baseDirectory(_/"target/generated-sources/").value
(sourceDirectory in avroConfig) := baseDirectory(_/"src/schemas").value

(compile in Compile) := ((compile in Compile) dependsOn (clean in Compile)).value


resolvers ++= Seq(
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  "Maven Central" at "http://repo1.maven.org/maven2",
  "Fuse Snapshots" at "http://repo.fusesource.com/nexus/content/repositories/snapshots",
  "Fuse" at "http://repo.fusesource.com/nexus/content/groups/public",
  "Confluent Maven Repo" at "http://packages.confluent.io/maven/"
)

// Remove scala dependency for pure Java libraries
autoScalaLibrary := false

// Remove the scala version from the generated/published artifact
crossPaths := false

publishMavenStyle := true

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

pomIncludeRepository := { _ => false }

pomExtra := (
  <url>https://github.com/dcshock/forklift-kafka</url>
  <licenses>
    <license>
      <name>BSD-style</name>
      <url>http://www.opensource.org/licenses/bsd-license.php</url>
      <distribution>repo</distribution>
    </license>
  </licenses>
  <scm>
    <url>git@github.com:dcshock/forklift-kafka.git</url>
    <connection>scm:git:git@github.com:dcshock/forklift-kafka.git</connection>
  </scm>
  <developers>
    <developer>
      <id>dcshock</id>
      <name>Matt Conroy</name>
      <url>http://www.mattconroy.com</url>
    </developer>
    <developer>
      <id>afrieze</id>
      <name>Andrew Frieze</name>
    </developer>
    <developer>
      <id>kuroshii</id>
      <name>Bridger Howell</name>
    </developer>
  </developers>)

useGpg := true
