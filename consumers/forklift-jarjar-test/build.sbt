import com.github.dcshock.SbtBinks._

organization := "com.github.dcshock"

name := "forklift-jarjar-consumer"
version := "0.3"

libraryDependencies ++= Seq(
    "com.github.dcshock" % "forklift" % "0.3" % "provided" intransitive(),
    "com.github.dcshock" % "forklift-multitq-consumer" % "[0.1,)" intransitive(),
    "com.github.dcshock" % "forklift-test-consumer" % "[0.1,)" intransitive()
)

crossPaths := false

resolvers ++= Seq(
    "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
    "Maven Central" at "http://repo1.maven.org/maven2",
    "Fuse Snapshots" at "http://repo.fusesource.com/nexus/content/repositories/snapshots",
    "Fuse" at "http://repo.fusesource.com/nexus/content/groups/public"
)

publishMavenStyle := true

autoScalaLibrary := false

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
      <id>dthompson</id>
      <name>David Thompson</name>
      <url>https://github.com/applitect</url>
    </developer>
  </developers>)

binksSettings
