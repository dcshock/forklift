organization := "com.github.dcshock"

name := "forklift-kafka"

version := "0.1"

// target and Xlint cause sbt dist to fail
javacOptions ++= Seq("-source", "1.8")//, "-target", "1.8", "-Xlint")

javacOptions in compile ++= Seq("-g:lines,vars,source")

initialize := {
  val _ = initialize.value
  if (sys.props("java.specification.version") != "1.8")
    sys.error("Java 8 is required for this project.")
}

libraryDependencies ++= Seq(
  "com.github.dcshock" % "forklift" % "0.19" ,
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.7.3",
  "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % "2.7.3",
  "org.apache.kafka" % "kafka-clients" % "0.10.1.1-cp1",
  "io.confluent" % "kafka-avro-serializer" % "3.1.1"

)

lazy val testDependencies = Seq(
  "commons-io" % "commons-io" % "2.4" ,
  "com.novocode" % "junit-interface" % "0.11",
  "org.mockito"       % "mockito-core"            % "1.9.5"
)

libraryDependencies ++= testDependencies.map(_ % "test")

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
  <url>https://github.com/dcshock/forklift</url>
    <licenses>
      <license>
        <name>BSD-style</name>
        <url>http://www.opensource.org/licenses/bsd-license.php</url>
        <distribution>repo</distribution>
      </license>
    </licenses>
    <scm>
      <url>git@github.com:dcshock/forklift.git</url>
      <connection>scm:git:git@github.com:dcshock/forklift.git</connection>
    </scm>
    <developers>
      <developer>
        <id>dcshock</id>
        <name>Matt Conroy</name>
        <url>http://www.mattconroy.com</url>
      </developer>
    </developers>)

useGpg := true
