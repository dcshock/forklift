name := "forklift"

version := "0.1"

libraryDependencies ++= Seq(
    "org.springframework" % "spring-jms" % "3.2.4.RELEASE",
    "com.google.guava" % "guava" % "18.0",
    "ch.qos.logback" % "logback-classic" % "1.0.13",
    "org.apache.geronimo.specs" % "geronimo-jms_1.1_spec" % "1.1.1",
    "org.reflections" % "reflections" % "0.9.9-RC1",
    "com.novocode" % "junit-interface" % "0.10" % "test",
    "org.mockito" % "mockito-all" % "1.9.5" % "test"
)

crossPaths := false

resolvers ++= Seq(
    "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
    "Maven Central" at "http://repo1.maven.org/maven2",
    "Fuse Snapshots" at "http://repo.fusesource.com/nexus/content/repositories/snapshots",
    "Fuse" at "http://repo.fusesource.com/nexus/content/groups/public"
)

packageArchetype.java_application

