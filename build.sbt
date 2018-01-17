lazy val baseSettings = Seq(
  organization := "com.github.dcshock",
  version := "3.1",
  scalaVersion := "2.11.7",
  javacOptions ++= Seq("-source", "1.8"),
  javacOptions in compile ++= Seq("-g:lines,vars,source", "-deprecation"),
  javacOptions in doc += "-Xdoclint:none",
  crossPaths := false,
  autoScalaLibrary := false,
  resolvers ++= Seq(
    "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
    "Maven Central" at "http://repo1.maven.org/maven2",
    "Fuse Snapshots" at "http://repo.fusesource.com/nexus/content/repositories/snapshots",
    "Fuse" at "http://repo.fusesource.com/nexus/content/groups/public",
    "Confluent Maven Repo" at "http://packages.confluent.io/maven/"
  ),
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value)
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases"  at nexus + "service/local/staging/deploy/maven2")
  },
  publishMavenStyle := true,
  credentials += Credentials(
    "Sonatype Nexus Repository Manager",
    "oss.sonatype.org",
    sys.env.getOrElse("SONATYPE_USER", ""),
    sys.env.getOrElse("SONATYPE_PASS", "")
  ),
  useGpg := false,
  usePgpKeyHex("E46770E4F1ED27F3"),
  pgpPublicRing := file(sys.props("user.dir")) / "project" / ".gnupg" / "pubring.gpg",
  pgpSecretRing := file(sys.props("user.dir")) / "project" / ".gnupg" / "secring.gpg",
  pgpPassphrase := sys.env.get("GPG_PASS").map(_.toArray),
  pomIncludeRepository := { _ => false },
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
)

lazy val core = project in file("core") settings baseSettings
lazy val replay = project.dependsOn(core) in file("plugins/replay") settings baseSettings
lazy val retry = project.dependsOn(core) in file("plugins/retry") settings baseSettings
lazy val stats = project.dependsOn(core) in file("plugins/stats") settings baseSettings
lazy val activemq = project.dependsOn(core) in file("connectors/activemq") settings baseSettings
lazy val kafka = project.dependsOn(core) in file("connectors/kafka") settings baseSettings
lazy val server = project.dependsOn(core, replay, retry, stats, activemq, kafka) in file("server") settings baseSettings

lazy val rootSettings = baseSettings ++ Seq(
    skip in publish := true
)
lazy val root = project in file(".") aggregate(core, replay, retry, stats, activemq, kafka, server) settings rootSettings
