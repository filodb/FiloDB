import sbt._
import sbt.Keys._

publishTo := Some(Resolver.file("Unused repo", file("target/unusedrepo")))


// Global setting across all subprojects
ThisBuild / organization := "org.filodb"
ThisBuild / organizationName := "FiloDB"
ThisBuild / scalaVersion := "2.13.14"
ThisBuild / publishMavenStyle := true
ThisBuild / Test / publishArtifact := false
ThisBuild / IntegrationTest / publishArtifact := false
ThisBuild / licenses += ("Apache-2.0", url("http://choosealicense.com/licenses/apache/"))
ThisBuild / pomIncludeRepository := { x => false }

// Force consistent dependency versions to prevent cross-version conflicts
ThisBuild / dependencyOverrides ++= Seq(
  "org.typelevel" %% "cats-kernel" % "2.10.0",
  "org.typelevel" %% "cats-core" % "2.10.0",
  "io.circe" %% "circe-core" % "0.12.3",
  "io.circe" %% "circe-generic" % "0.12.3",
  "io.circe" %% "circe-parser" % "0.12.3",
  "io.circe" %% "circe-jawn" % "0.12.3",
  "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2",
  "org.scala-lang.modules" %% "scala-xml" % "2.1.0",
  "com.typesafe.akka" %% "akka-actor" % "2.5.32",
  "com.typesafe.akka" %% "akka-stream" % "2.5.32"
)

// protobuf-java 3.23.1 for every module: that is what Arrow 15.0.2 compiles flight-core against
// (arrow-java-root pom: dep.protobuf-bom.version), and below 3.22 LazyStringArrayList.emptyList()
// is package-private, so Flight$FlightDescriptor fails static init with IllegalAccessError and
// every Flight suite aborts. It must also stay below 4.x, which drops makeExtensionsImmutable()
// and breaks the checked-in prometheus/.../remote/RemoteStorage.java (generated 2.5.0 gencode).
//
// Pinned per-project rather than at ThisBuild on purpose: sbt-protoc resolves the classpath for
// gateway's scalapb.gen() code generator from ThisBuild / dependencyOverrides, and ScalaPB
// compilerplugin 0.11.11 breaks on >= 3.22 (it calls FieldOptions.Builder.setExtension, whose
// covariant override was removed in 3.22). With no protobuf entry at ThisBuild, that generator
// resolves ScalaPB's own 3.19.6 and keeps working, while every module still builds and runs on
// 3.23.1.
lazy val protobufPin = Seq(
  dependencyOverrides += "com.google.protobuf" % "protobuf-java" % "3.23.1"
)

enablePlugins(ProtobufPlugin)

lazy val memory = Submodules.memory.disablePlugins(SonarPlugin).settings(protobufPin: _*)
lazy val core = Submodules.core.disablePlugins(SonarPlugin).settings(protobufPin: _*)
lazy val query = Submodules.query.disablePlugins(SonarPlugin).settings(protobufPin: _*)
lazy val prometheus = Submodules.prometheus.disablePlugins(SonarPlugin).settings(protobufPin: _*)
lazy val coordinator = Submodules.coordinator.disablePlugins(SonarPlugin).settings(protobufPin: _*)
lazy val cassandra = Submodules.cassandra.disablePlugins(SonarPlugin).settings(protobufPin: _*)
lazy val kafka = Submodules.kafka.disablePlugins(SonarPlugin).settings(protobufPin: _*)
lazy val cli = Submodules.cli.disablePlugins(SonarPlugin).settings(protobufPin: _*)
lazy val http = Submodules.http.disablePlugins(SonarPlugin).settings(protobufPin: _*)
// Arrow reaches gateway only transitively through coordinator and nothing under gateway/src
// references Flight, so exclude it: gateway neither needs nor ships Arrow. With Arrow gone, gateway
// has no reason to be on 3.23.1 either — it stays on 3.21.7, matching its ScalaPB gencode and the
// protobuf its downstream consumers resolve. protobuf-java is declared explicitly so the published
// POM states a real version instead of the stale 2.5.0 in Dependencies.scala's gatewayDeps.
lazy val gateway = Submodules.gateway.disablePlugins(SonarPlugin)
  .settings(
    dependencyOverrides += "com.google.protobuf" % "protobuf-java" % "3.21.7",
    excludeDependencies += ExclusionRule("org.apache.arrow"),
    libraryDependencies += "com.google.protobuf" % "protobuf-java" % "3.21.7"
  )
lazy val standalone = Submodules.standalone.disablePlugins(SonarPlugin).settings(protobufPin: _*)
lazy val bootstrapper = Submodules.bootstrapper.disablePlugins(SonarPlugin).settings(protobufPin: _*)
lazy val sparkJobs = Submodules.sparkJobs.disablePlugins(SonarPlugin).settings(protobufPin: _*)
lazy val jmh = Submodules.jmh.disablePlugins(SonarPlugin).settings(protobufPin: _*)
lazy val gatling = Submodules.gatling.disablePlugins(SonarPlugin).settings(protobufPin: _*)
lazy val grpc = Submodules.grpc.disablePlugins(SonarPlugin).settings(protobufPin: _*)


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
