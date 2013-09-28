name := "forklift"

version := "0.1"

libraryDependencies ++= Seq(
    "junit" % "junit" % "4.11" % "test",
    "com.novocode" % "junit-interface" % "0.8" % "test->default",
    "org.springframework" % "spring-jms" % "3.2.4.RELEASE",
    "ch.qos.logback" % "logback-classic" % "1.0.13",
    "org.apache.geronimo.specs" % "geronimo-jms_1.1_spec" % "1.1.1"
)

autoScalaLibrary := false

crossPaths := false
