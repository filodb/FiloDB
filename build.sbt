import sbt._
import sbt.Keys._

publishTo := Some(Resolver.file("Unused repo", file("target/unusedrepo")))


// Global setting across all subprojects
ThisBuild / organization := "org.filodb"
ThisBuild / organizationName := "FiloDB"
ThisBuild / scalaVersion := "2.12.12"
ThisBuild / publishMavenStyle := true
ThisBuild / Test / publishArtifact := false
ThisBuild / IntegrationTest / publishArtifact := false
ThisBuild / licenses += ("Apache-2.0", url("http://choosealicense.com/licenses/apache/"))
ThisBuild / pomIncludeRepository := { x => false }

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
    sparkJobs,
    grpc,
    bootstrapper,
    gatling,
    jmh
  )
