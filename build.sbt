val mySettings = Seq(organization := "org.velvia",
                     scalaVersion := "2.10.4",
                     parallelExecution in Test := false,
                     resolvers ++= extraRepos) ++
                 universalSettings

lazy val core = (project in file("core"))
                  .settings(mySettings:_*)
                  .settings(name := "filodb-core")
                  .settings(scalacOptions += "-language:postfixOps")
                  .settings(libraryDependencies ++= coreDeps)

lazy val coordinator = (project in file("coordinator"))
                         .settings(mySettings:_*)
                         .settings(name := "filodb-coordinator")
                         .settings(libraryDependencies ++= coordDeps)
                         .dependsOn(core % "compile->compile; test->test",
                                    cassandra % "compile->compile; test->test")

lazy val cassandra = (project in file("cassandra"))
                       .settings(mySettings:_*)
                       .settings(name := "filodb-cassandra")
                       .settings(libraryDependencies ++= cassDeps)
                       .dependsOn(core % "compile->compile; test->test")

lazy val cli = (project in file("cli"))
                 .settings(mySettings:_*)
                 .settings(name := "filodb-cli")
                 .settings(libraryDependencies ++= cliDeps)
                 .settings(cliAssemblySettings:_*)
                 .dependsOn(core)

lazy val spark = (project in file("spark"))
                   .settings(mySettings:_*)
                   .settings(name := "filodb-spark")
                   .settings(libraryDependencies ++= sparkDeps)
                   .settings(assemblySettings:_*)
                   .settings(assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false))
                   .dependsOn(core % "compile->compile; test->test")

val phantomVersion = "1.11.0"
val akkaVersion    = "2.3.7"

lazy val extraRepos = Seq(
  "Typesafe repository releases" at "http://repo.typesafe.com/typesafe/releases/",
  "twitter-repo" at "http://maven.twttr.com",
  "Websudos releases" at "https://dl.bintray.com/websudos/oss-releases/",
  "Pellucid Bintray" at "http://dl.bintray.com/pellucid/maven",
  "Velvia Bintray" at "https://dl.bintray.com/velvia/maven",
  "spray repo" at "http://repo.spray.io"
)

val excludeShapeless = ExclusionRule(organization = "com.chuusai")
// Zookeeper pulls in slf4j-log4j12 which we DON'T want
val excludeZK = ExclusionRule(organization = "org.apache.zookeeper")

lazy val coreDeps = Seq(
  "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2",
  "ch.qos.logback"        % "logback-classic"   % "1.0.7",
  "com.beachape"         %% "enumeratum"        % "1.2.1",
  "org.velvia.filo"      %% "filo-scala"        % "0.1.3" excludeAll(excludeShapeless),
  "io.spray"             %% "spray-caching"     % "1.3.2",
  "org.mapdb"             % "mapdb"             % "1.0.6",
  "net.ceedubs"          %% "ficus"             % "1.0.1",
  "com.nativelibs4java"  %% "scalaxy-loops"     % "0.3.3" % "provided",
  "org.scalatest"        %% "scalatest"         % "2.2.4" % "test"
)

lazy val cassDeps = Seq(
  "com.websudos"         %% "phantom-dsl"       % phantomVersion,
  "com.websudos"         %% "phantom-testkit"   % phantomVersion % "test" excludeAll(excludeZK)
)

lazy val coordDeps = Seq(
  "com.typesafe.akka"    %% "akka-slf4j"        % akkaVersion,
  "com.opencsv"           % "opencsv"           % "3.3",
  "com.typesafe.akka"    %% "akka-testkit"      % akkaVersion % "test",
  "org.scalatest"        %% "scalatest"         % "2.2.4" % "test"
)

lazy val cliDeps = Seq(
  "com.quantifind"       %% "sumac"             % "0.3.0"
)

lazy val sparkDeps = Seq(
  "org.apache.spark"     %% "spark-sql"         % "1.4.0" % "provided"
)

//////////////////////////
///

lazy val coreSettings = Seq(
  scalacOptions ++= Seq("-Xlint", "-deprecation", "-Xfatal-warnings", "-feature")
)

lazy val universalSettings = coreSettings ++ styleSettings

// Create a default Scala style task to run with tests
lazy val testScalastyle = taskKey[Unit]("testScalastyle")

lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")

lazy val styleSettings = Seq(
  testScalastyle := org.scalastyle.sbt.ScalastylePlugin.scalastyle.in(Test).toTask("").value,
  // (scalastyleConfig in Test) := "scalastyle-test-config.xml",
  // This is disabled for now, cannot get ScalaStyle to recognize the file above for some reason :/
  // (test in Test) <<= (test in Test) dependsOn testScalastyle,
  scalastyleFailOnError := true,
  compileScalastyle := org.scalastyle.sbt.ScalastylePlugin.scalastyle.in(Compile).toTask("").value,
  // Is running this on compile too much?
  (compile in Test) <<= (compile in Test) dependsOn compileScalastyle
)

lazy val shellScript = """#!/usr/bin/env sh
exec java -jar "$0" "$@"
""".split("\n")

// Builds cli as a standalone executable to make it easier to launch commands
lazy val cliAssemblySettings = Seq(
  assemblyOption in assembly := (assemblyOption in assembly).value.copy(
                                  prependShellScript = Some(shellScript)),
  assemblyJarName in assembly := s"filo-cli-${version.value}"
)

lazy val assemblySettings = Seq(
  assemblyMergeStrategy in assembly := {
    case PathList(ps @ _*) if ps.last endsWith ".txt.1" => MergeStrategy.first
    case "application.conf"                            => MergeStrategy.concat
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  }
)
