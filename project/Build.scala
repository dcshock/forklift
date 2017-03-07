import sbt._
import Keys._

object ForkliftBuild extends Build {
    lazy val core = Project(
        id = "core",
        base = file("core")
    )

    lazy val pluginNotify = Project(
        id = "notify",
        base = file("plugins/notify")
    ).dependsOn(core)

    lazy val replay = Project(
        id = "replay",
        base = file("plugins/replay")
    ).dependsOn(core)

    lazy val retry = Project(
        id = "retry",
        base = file("plugins/retry")
    ).dependsOn(core)

    lazy val stats = Project(
        id = "stats",
        base = file("plugins/stats")
    ).dependsOn(core)

    lazy val activemq = Project(
        id = "activemq",
        base = file("connectors/activemq")
    ).dependsOn(core)

    lazy val kafka = Project(
        id = "kafka",
        base = file("connectors/kafka")
    ).dependsOn(core)

    lazy val server = Project(
        id = "server",
        base = file("server")
    ).dependsOn(core, activemq, replay, retry, kafka)

}
