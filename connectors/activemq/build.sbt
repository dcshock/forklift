name := "forklift-activemq"

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
    "forklift" % "forklift" % "0.1",
    "org.springframework" % "spring-jms" % "4.1.1.RELEASE",
    "ch.qos.logback" % "logback-classic" % "1.0.13",
    "org.apache.geronimo.specs" % "geronimo-jms_1.1_spec" % "1.1.1",
    "org.apache.activemq" % "activemq-all" % "5.8.0",
    "commons-io" % "commons-io" % "2.4" % "test",
    "com.novocode" % "junit-interface" % "0.10" % "test"
)

crossPaths := false

resolvers ++= Seq(
    "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
    "Maven Central" at "http://repo1.maven.org/maven2",
    "Fuse Snapshots" at "http://repo.fusesource.com/nexus/content/repositories/snapshots",
    "Fuse" at "http://repo.fusesource.com/nexus/content/groups/public"
)
