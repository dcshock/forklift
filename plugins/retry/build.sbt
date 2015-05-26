organization := "com.github.dcshock"

name := "forklift-retry"

version := "0.1"

scalaVersion := "2.11.4"

javacOptions ++= Seq("-source", "1.8")

initialize := {
  val _ = initialize.value
  if (sys.props("java.specification.version") != "1.8")
    sys.error("Java 8 is required for this project.")
}

libraryDependencies ++= Seq(
  "com.github.dcshock" % "forklift" % "0.1"
)

crossPaths := false

resolvers ++= Seq(
    "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
    "Maven Central" at "http://repo1.maven.org/maven2",
    "Fuse Snapshots" at "http://repo.fusesource.com/nexus/content/repositories/snapshots",
    "Fuse" at "http://repo.fusesource.com/nexus/content/groups/public"
)
