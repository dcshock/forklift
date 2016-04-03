organization := "com.github.dcshock"

name := "forklift-server"

version := "0.18"

enablePlugins(JavaAppPackaging)

javacOptions ++= Seq("-source", "1.8")

initialize := {
  val _ = initialize.value
  if (sys.props("java.specification.version") != "1.8")
    sys.error("Java 8 is required for this project.")
}

libraryDependencies ++= Seq(
  "com.github.dcshock" % "forklift"           % "0.14",
  "com.github.dcshock" % "forklift-activemq"  % "0.7",
  "com.github.dcshock" % "forklift-replay"    % "0.7",
  "com.github.dcshock" % "forklift-retry"     % "0.9",
  "com.github.dcshock" % "consul-rest-client" % "0.6",
  "io.searchbox" % "jest" % "2.0.0",
  "org.apache.activemq" % "activemq-all" % "5.8.0",
  "org.apache.geronimo.specs" % "geronimo-jms_1.1_spec" % "1.1.1",
  "args4j" % "args4j" % "2.0.31",
  "org.codehaus.janino" % "janino" % "2.6.1",
  "ch.qos.logback" % "logback-classic" % "1.1.2",
  "ch.qos.logback.contrib" % "logback-json-core"    % "0.1.2",
  "ch.qos.logback.contrib" % "logback-json-classic" % "0.1.2",
  "ch.qos.logback.contrib" % "logback-jackson"      % "0.1.2",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.5.1",
  "javax.inject" % "javax.inject" % "1",
  "com.novocode" % "junit-interface" % "0.11" % "test",
  "commons-io" % "commons-io" % "2.4" % "test"
)

resolvers ++= Seq(
    "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
    "Maven Central" at "http://repo1.maven.org/maven2",
    "Fuse Snapshots" at "http://repo.fusesource.com/nexus/content/repositories/snapshots",
    "Fuse" at "http://repo.fusesource.com/nexus/content/groups/public"
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
