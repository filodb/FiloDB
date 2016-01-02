import sbt.Keys._


val mySettings = Seq(organization := "org.velvia",
                     scalaVersion := "2.10.4",
                     parallelExecution in Test := false,
                     fork in Test := true,
                     resolvers ++= extraRepos,
                     ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }) ++ universalSettings

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
                                    cassandra % "test->test")

lazy val cassandra = (project in file("cassandra"))
                       .settings(mySettings:_*)
                       .settings(name := "filodb-cassandra")
                       .settings(libraryDependencies ++= cassDeps)
                       .dependsOn(core % "compile->compile; test->test")

lazy val cli = (project in file("cli"))
                 .settings(mySettings:_*)
                 .settings(name := "filodb-cli")
                 .settings(libraryDependencies ++= cliDeps)
                 .enablePlugins(JavaAppPackaging)
                 .enablePlugins(UniversalPlugin)
                 .settings(mappings in Universal  := {
                    val universalMappings = (mappings in Universal).value
                    val fatJar = (assembly in spark).value
                    universalMappings :+ (fatJar -> ("lib/" + fatJar.getName))})
                 .settings(bashScriptExtraDefines +=
                   """addJava "-DaddedJar=${lib_dir}/""" + s"""filodb-spark-assembly-${version.value}.jar"""".trim)
                 .settings(batScriptExtraDefines +=
                   s"""set _JAVA_OPTS=%_JAVA_OPTS% -DaddedJar=%APP_LIB_DIR%\\filodb-spark-assembly-${version.value}.jar""")
                 .settings(Distribution.cliAssemblySettings:_*)
                 .dependsOn(core, coordinator, cassandra,spark)

lazy val spark = (project in file("spark"))
                   .settings(mySettings:_*)
                   .settings(name := "filodb-spark")
                   .settings(libraryDependencies ++= sparkDeps)
                   .settings(assemblySettings:_*)
                   .settings(assemblyExcludedJars in assembly := { val cp = (fullClasspath
                     in assembly).value
                     val excludesJar = Seq("logback-classic-1.1.2.jar")
                     cp filter { jar => excludesJar.contains(jar.data.getName)}
                   })
                   .settings(assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = true))
                   .dependsOn(core % "compile->compile; test->test")
                   .dependsOn(cassandra % "compile->compile; test->test")
                   .dependsOn(coordinator % "compile->compile; test->test")

lazy val perfTests = (project in file("perftests"))
                   .settings(mySettings: _*)
                   .settings(name := "filodb-perf")
                   .settings(libraryDependencies ++= perfDeps)
                   .dependsOn(core, coordinator, cassandra, spark % "compile->compile; test->test")

val phantomVersion = "1.12.2"
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
  "org.slf4j" % "slf4j-log4j12" % "1.7.10",
  "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2",
  "com.clearspring.analytics" % "stream"        % "2.7.0",
  "it.unimi.dsi"          % "dsiutils"          % "2.2.4",
  //"ch.qos.logback"        % "logback-classic"   % "1.0.7",
  "com.beachape"         %% "enumeratum"        % "1.2.1",
  "org.velvia.filo"      %% "filo-scala"        % "0.2.0",
  "io.spray"             %% "spray-caching"     % "1.3.2",
  "org.mapdb"             % "mapdb"             % "1.0.6",
  "org.velvia"           %% "msgpack4s"         % "0.5.1",
  "org.scodec"           %% "scodec-bits"       % "1.0.10",
  "com.nativelibs4java"  %% "scalaxy-loops"     % "0.3.3" % "provided",
  "org.scalatest"        %% "scalatest"         % "2.2.4" % "test"
)

lazy val cassDeps = Seq(
  "com.websudos"                  %% "phantom-dsl"              % phantomVersion,
  "com.fasterxml.jackson.core"    % "jackson-databind"          % "2.4.1.1",
  "com.fasterxml.jackson.module"  % "jackson-module-scala_2.10" % "2.4.1",
  "org.cassandraunit"             % "cassandra-unit"            % "2.0.2.2"       % "test",
  "com.websudos"                  %% "phantom-testkit"          % phantomVersion  % "test" excludeAll(excludeZK)
)

lazy val coordDeps = Seq(
  "com.typesafe.akka"    %% "akka-slf4j"        % akkaVersion,
  "com.opencsv"           % "opencsv"           % "3.3",
  "com.typesafe.akka"    %% "akka-testkit"      % akkaVersion % "test",
  "org.scalatest"        %% "scalatest"         % "2.2.4" % "test"
)

lazy val cliDeps = Seq(
  ("org.apache.spark"     %% "spark-sql"         % "1.4.1").
    exclude("org.mortbay.jetty", "servlet-api").
    exclude("commons-beanutils", "commons-beanutils-core").
    exclude("commons-collections", "commons-collections").
    exclude("commons-logging", "commons-logging").
    exclude("com.esotericsoftware.minlog", "minlog"),
  "org.jboss.aesh"        % "aesh"              % "0.66",
  "com.github.lalyos"     % "jfiglet"           % "0.0.7",
  "org.jboss.aesh"        % "aesh-extensions"   % "0.66"
)

lazy val perfDeps = Seq(
  "com.databricks"       %% "spark-csv"         % "1.3.0",
  "org.apache.spark"     %% "spark-sql"         % "1.4.1"
)

lazy val sparkDeps = Seq(
  "org.apache.spark"     %% "spark-sql"         % "1.4.1"  % "provided"
)

lazy val coreSettings = Seq(
  scalacOptions ++= Seq("-Xlint", "-deprecation", "-Xfatal-warnings", "-feature"))

lazy val testSettings = Seq(
    parallelExecution in Test := false,
    concurrentRestrictions in Global := Seq(
      // Tags.limit(Tags.CPU, java.lang.Runtime.getRuntime().availableProcessors()),
      Tags.limit(Tags.CPU, 1),
      // limit to 1 concurrent test task, even across sub-projects
      Tags.limit(Tags.Test, 1),
      // Note: some components of tests seem to have the "Untagged" tag rather than "Test" tag.
      // So, we limit the sum of "Test", "Untagged" tags to 1 concurrent
      Tags.limitSum(1, Tags.Test, Tags.Untagged))
)

lazy val universalSettings = coreSettings ++ styleSettings ++ testSettings

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

lazy val assemblySettings = Seq(
  assemblyMergeStrategy in assembly := {
    case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
    case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
    case m if m.toLowerCase.matches("meta-inf.*\\.properties") => MergeStrategy.discard
    case PathList(ps @ _*) if ps.last endsWith ".txt.1" => MergeStrategy.first
      case "reference.conf" => MergeStrategy.concat
    case "application.conf"                            => MergeStrategy.concat
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  },
  test in assembly := {} //noisy for end-user since the jar is not available and user needs to build the project locally
)
val zip = TaskKey[File]("zip", "Creates a distributable zip file.")

zip <<= Distribution.zipTask

addCommandAlias("distZip", ";clean;spark/assembly;cli/assembly;zip")

addCommandAlias("dist", ";clean;cli/stage;cli/universal:packageBin")
