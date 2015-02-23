name := "forklift"

version := "0.1"

scalaVersion := "2.11.4"

// target and Xlint cause sbt dist to fail
javacOptions ++= Seq("-source", "1.8")//, "-target", "1.8", "-Xlint")

initialize := {
  val _ = initialize.value
  if (sys.props("java.specification.version") != "1.8")
    sys.error("Java 8 is required for this project.")
}

libraryDependencies ++= Seq(
    "org.springframework" % "spring-jms" % "4.1.1.RELEASE",
    "com.google.guava" % "guava" % "18.0",
    "ch.qos.logback" % "logback-classic" % "1.0.13",
    "org.apache.geronimo.specs" % "geronimo-jms_1.1_spec" % "1.1.1",
    "org.reflections" % "reflections" % "0.9.9-RC1",
    "com.novocode" % "junit-interface" % "0.10" % "test",
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.5.1",
    "org.mockito" % "mockito-all" % "1.9.5" % "test"
)

crossPaths := false

resolvers ++= Seq(
    "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
    "Maven Central" at "http://repo1.maven.org/maven2",
    "Fuse Snapshots" at "http://repo.fusesource.com/nexus/content/repositories/snapshots",
    "Fuse" at "http://repo.fusesource.com/nexus/content/groups/public"
)

