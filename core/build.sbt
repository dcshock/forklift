organization := "com.github.dcshock"

name := "forklift"

version := "2.2"

javacOptions ++= Seq("-source", "1.8")

javacOptions in compile ++= Seq("-g:lines,vars,source", "-deprecation")

javacOptions in doc += "-Xdoclint:none"

initialize := {
  val _ = initialize.value
  if (sys.props("java.specification.version") != "1.8")
    sys.error("Java 8 is required for this project.")
}

resolvers ++= Seq(
    "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
    "Maven Central" at "http://repo1.maven.org/maven2",
    "Fuse Snapshots" at "http://repo.fusesource.com/nexus/content/repositories/snapshots",
    "Fuse" at "http://repo.fusesource.com/nexus/content/groups/public"
)

libraryDependencies ++= Seq(
    "com.google.guava" % "guava" % "18.0",
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.7.3",
    "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % "2.7.3",
    "ch.qos.logback" % "logback-classic" % "1.0.13",
    "org.reflections" % "reflections" % "0.9.10",
    "javax.inject" % "javax.inject" % "1"
)

lazy val testDependencies = Seq(
    "com.novocode" % "junit-interface" % "0.11",
    "org.mockito" % "mockito-all" % "1.9.5"
)

libraryDependencies ++= testDependencies.map(_ % "test")

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
    <developer>
      <id>afrieze</id>
      <name>Andrew Frieze</name>
    </developer>
    <developer>
      <id>kuroshii</id>
      <name>Bridger Howell</name>
    </developer>
  </developers>)
