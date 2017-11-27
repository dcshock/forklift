name := "forklift-replay"

libraryDependencies ++= Seq(
  "org.elasticsearch" % "elasticsearch" % "2.4.1",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.7.3",
  "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % "2.7.3"
)

lazy val testDependencies = Seq(
  "com.novocode" % "junit-interface" % "0.11",
  "org.mockito" % "mockito-all" % "1.9.5"
)

libraryDependencies ++= testDependencies.map(_ % "test")
