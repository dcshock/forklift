name := "forklift"

version := "0.1"

libraryDependencies += "junit" % "junit" % "4.11" % "test"

libraryDependencies += "com.novocode" % "junit-interface" % "0.8" % "test->default"

libraryDependencies += "org.springframework" % "spring-jms" % "3.2.4.RELEASE"

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.0.13"

autoScalaLibrary := false

crossPaths := false
