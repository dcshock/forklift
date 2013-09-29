import sbt._
import Keys._

object ForkliftBuild extends Build {
    lazy val core = Project(
        id = "core",
        base = file("./core")
    )
}
