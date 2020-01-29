import Dependencies._
import FiloSettings._
import com.typesafe.sbt.SbtMultiJvm.MultiJvmKeys.MultiJvm
import pl.project13.scala.sbt.JmhPlugin
import sbt._
import sbt.Keys._

publishTo := Some(Resolver.file("Unused repo", file("target/unusedrepo")))

// Global setting across all subprojects
ThisBuild / organization := "org.filodb"
ThisBuild / organizationName := "FiloDB"
ThisBuild / scalaVersion := "2.11.12"
ThisBuild / publishMavenStyle := true
ThisBuild / Test / publishArtifact := false
ThisBuild / IntegrationTest / publishArtifact := false
ThisBuild / licenses += ("Apache-2.0", url("http://choosealicense.com/licenses/apache/"))
ThisBuild / pomIncludeRepository := { x => false }


/**
  * FiloDB modules and dependencies
  */

lazy val memory = (project in file("memory"))
  .settings(
    commonSettings,
    assemblySettings,
    name := "filodb-memory",
    scalacOptions += "-language:postfixOps",
    libraryDependencies ++= memoryDeps
  )

lazy val core = (project in file("core"))
  .dependsOn(memory % "compile->compile; test->test")
  .settings(
    commonSettings,
    name := "filodb-core",
    scalacOptions += "-language:postfixOps",
    libraryDependencies ++= coreDeps
  )

lazy val coordinator = (project in file("coordinator"))
  .dependsOn(core % "compile->compile; test->test")
  .dependsOn(query % "compile->compile; test->test")
  .dependsOn(prometheus % "compile->compile; test->test")
  .configs(MultiJvm)
  .settings(
    commonSettings,
    multiJvmSettings,
    testMultiJvmToo,
    name := "filodb-coordinator",
    libraryDependencies ++= coordDeps,
    libraryDependencies +=
    "com.typesafe.akka" %% "akka-contrib" % akkaVersion exclude(
      "com.typesafe.akka", s"akka-persistence-experimental_${scalaBinaryVersion.value}")
  )

lazy val prometheus = (project in file("prometheus"))
  .dependsOn(core % "compile->compile; test->test")
  .dependsOn(query % "compile->compile; test->test")
  .settings(
    commonSettings,
    name := "filodb-prometheus",
    libraryDependencies ++= promDeps
  )

lazy val query = (project in file("query"))
  .dependsOn(core % "compile->compile; test->test")
  .settings(
    libraryDependencies ++= queryDeps,
    commonSettings,
    scalacOptions += "-language:postfixOps",
    name := "filodb-query"
  )

lazy val cassandra = (project in file("cassandra"))
  .dependsOn(core % "compile->compile; test->test", coordinator)
  .settings(
    commonSettings,
    name := "filodb-cassandra",
    libraryDependencies ++= cassDeps
  )

lazy val cli = (project in file("cli"))
  .dependsOn(prometheus % "compile->compile; test->test")
  .dependsOn(core, coordinator % "test->test", cassandra)
  .settings(
    commonSettings,
    name := "filodb-cli",
    libraryDependencies ++= cliDeps,
    cliAssemblySettings
  )

lazy val kafka = (project in file("kafka"))
  .dependsOn(
    core % "compile->compile; it->test",
    coordinator % "compile->compile; test->test"
  )
  .configs(IntegrationTest, MultiJvm)
  .settings(
    name := "filodb-kafka",
    commonSettings,
    kafkaSettings,
    itSettings,
    assemblySettings,
    libraryDependencies ++= kafkaDeps
  )

lazy val sparkJobs = (project in file("spark-jobs"))
  .dependsOn(cassandra, core % "compile->compile; test->test")
  .settings(
    commonSettings,
    name := "spark-jobs",
    fork in Test := true,
    baseDirectory in Test := file("."),   // since we have a config using FiloDB project root as relative path
    assemblySettings,
    scalacOptions += "-language:postfixOps",
    libraryDependencies ++= sparkJobsDeps
  )

lazy val bootstrapper = (project in file("akka-bootstrapper"))
  .configs(MultiJvm)
  .settings(
    commonSettings,
    multiJvmMaybeSettings,
    name := "akka-bootstrapper",
    libraryDependencies ++= bootstrapperDeps
  )

lazy val http = (project in file("http"))
  .dependsOn(core, coordinator % "compile->compile; test->test")
  .settings(
    commonSettings,
    name := "http",
    libraryDependencies ++= httpDeps
  )

lazy val standalone = (project in file("standalone"))
  .dependsOn(core, prometheus % "test->test", coordinator % "compile->compile; test->test",
    cassandra, kafka, http, bootstrapper, gateway % Test)
  .configs(MultiJvm)
  .settings(
    commonSettings,
    multiJvmMaybeSettings,
    assemblySettings,
    libraryDependencies ++= standaloneDeps
  )

// standalone does not depend on spark-jobs, but the idea is to simplify packaging and versioning

//  lazy val spark = (project in file("spark"))
//    .dependsOn(core % "compile->compile; test->test; it->test",
//      coordinator % "compile->compile; test->test",
//      cassandra % "compile->compile; test->test; it->test")
//    .configs( IntegrationTest )
//    .settings(
//      name := "filodb-spark",
//      commonSettings,
//      libraryDependencies ++= sparkDeps,
//      itSettings,
//      jvmPerTestSettings,
//      assemblyExcludeScala,
//    // Disable tests for now since lots of work remaining to enable Spark
//      test := {}
//    )

lazy val jmh = (project in file("jmh"))
  .enablePlugins(JmhPlugin)
  .dependsOn(core % "compile->compile; compile->test", gateway)
  .settings(
    commonSettings,
    name := "filodb-jmh",
    libraryDependencies ++= jmhDeps,
    publish := {}
  )

//  lazy val stress = (project in file("stress"))
//    .dependsOn(spark)
//    .settings(
//      commonSettings,
//      name := "filodb-stress",
//      libraryDependencies ++= stressDeps,
//      assemblyExcludeScala
//    )

lazy val gateway = (project in file("gateway"))
  .dependsOn(coordinator % "compile->compile; test->test",
    prometheus, cassandra)
  .settings(
    commonSettings,
    name := "filodb-gateway",
    libraryDependencies ++= gatewayDeps,
    gatewayAssemblySettings
  )
