name := "forklift"

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
