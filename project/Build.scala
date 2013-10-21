import sbt._
import Keys._

object ForkliftBuild extends Build {
    lazy val core = Project(
        id = "core",
        base = file("core")
    )

    lazy val activemq = Project(
        id = "activemq",
        base = file("connectors/activemq")
    ).dependsOn(core)
}
