import sbt._
import sbt.Keys._

publishTo := Some(Resolver.file("Unused repo", file("target/unusedrepo")))


// Global setting across all subprojects
ThisBuild / organization := "org.filodb"
ThisBuild / organizationName := "FiloDB"
ThisBuild / scalaVersion := "2.13.12"
ThisBuild / publishMavenStyle := true
ThisBuild / Test / publishArtifact := false
ThisBuild / licenses += ("Apache-2.0", url("http://choosealicense.com/licenses/apache/"))
ThisBuild / pomIncludeRepository := { x => false }

// Force Scala 2.13 versions to prevent cross-version conflicts
// Also force consistent circe versions (sttp 1.7.2 requires 0.12.x)
ThisBuild / dependencyOverrides ++= Seq(
  "org.typelevel" %% "cats-kernel" % "2.10.0",
  "org.typelevel" %% "cats-core" % "2.10.0",
  "io.circe" %% "circe-core" % "0.12.3",
  "io.circe" %% "circe-generic" % "0.12.3",
  "io.circe" %% "circe-parser" % "0.12.3",
  "io.circe" %% "circe-jawn" % "0.12.3",
  "org.scala-lang.modules" %% "scala-parser-combinators" % "2.3.0"
)

// Globally exclude kryo-shaded to prevent conflicts with kryo 5.5.0
ThisBuild / excludeDependencies ++= Seq(
  ExclusionRule("com.esotericsoftware", "kryo-shaded")
)

enablePlugins(ProtobufPlugin)

lazy val memory = Submodules.memory.disablePlugins(SonarPlugin)
lazy val core = Submodules.core.disablePlugins(SonarPlugin)
lazy val query = Submodules.query.disablePlugins(SonarPlugin)
lazy val prometheus = Submodules.prometheus.disablePlugins(SonarPlugin)
lazy val coordinator = Submodules.coordinator.disablePlugins(SonarPlugin)
lazy val cassandra = Submodules.cassandra.disablePlugins(SonarPlugin)
lazy val kafka = Submodules.kafka.disablePlugins(SonarPlugin)
lazy val cli = Submodules.cli.disablePlugins(SonarPlugin)
lazy val http = Submodules.http.disablePlugins(SonarPlugin)
lazy val gateway = Submodules.gateway.disablePlugins(SonarPlugin)
lazy val standalone = Submodules.standalone.disablePlugins(SonarPlugin)
lazy val bootstrapper = Submodules.bootstrapper.disablePlugins(SonarPlugin)
lazy val sparkJobs = Submodules.sparkJobs.disablePlugins(SonarPlugin)
lazy val jmh = Submodules.jmh.disablePlugins(SonarPlugin)
lazy val gatling = Submodules.gatling.disablePlugins(SonarPlugin)
lazy val grpc = Submodules.grpc.disablePlugins(SonarPlugin)


lazy val root = (project in file("."))
  .aggregate(
    memory,
    core,
    query,
    prometheus,
    coordinator,
    cassandra,
    kafka,
    cli,
    http,
    gateway,
    standalone,
    sparkJobs,  // Re-enabled with Spark 3.5.7 for Scala 2.13 compatibility
    grpc,
    bootstrapper,
    gatling,
    jmh
  )
