organization := "com.github.dcshock"

name := "forklift-notify"

version := "0.3"

javacOptions ++= Seq("-source", "1.8")

initialize := {
  val _ = initialize.value
  if (sys.props("java.specification.version") != "1.8")
    sys.error("Java 8 is required for this project.")
}

libraryDependencies ++= Seq(
  "com.github.dcshock" % "forklift" % "0.3"
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
