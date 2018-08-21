import com.typesafe.sbt.SbtMultiJvm
import com.typesafe.sbt.SbtMultiJvm.MultiJvmKeys.MultiJvm

val buildSettings = Seq(
  organization := "org.velvia",
  scalaVersion := "2.11.12")

publishTo      := Some(Resolver.file("Unused repo", file("target/unusedrepo")))

lazy val memory = project
  .in(file("memory"))
  .settings(commonSettings: _*)
  .settings(name := "filodb-memory")
  .settings(scalacOptions += "-language:postfixOps")
  .settings(libraryDependencies ++= memoryDeps)

lazy val core = project
  .in(file("core"))
  .settings(commonSettings: _*)
  .settings(name := "filodb-core")
  .settings(scalacOptions += "-language:postfixOps")
  .settings(libraryDependencies ++= coreDeps)
  .dependsOn(memory % "compile->compile; test->test")

lazy val coordinator = project
  .in(file("coordinator"))
  .settings(commonSettings: _*)
  .settings(multiJvmSettings: _*)
  .settings(testMultiJvmToo: _*)
  .settings(name := "filodb-coordinator")
  .settings(libraryDependencies ++= coordDeps)
  .settings(libraryDependencies +=
    "com.typesafe.akka" %% "akka-contrib" % akkaVersion exclude(
      "com.typesafe.akka", s"akka-persistence-experimental_${scalaBinaryVersion.value}"))
  .dependsOn(core % "compile->compile; test->test")
  .dependsOn(query % "compile->compile; test->test")
  .dependsOn(prometheus % "test->test")
  .configs(MultiJvm)

lazy val prometheus = project
  .in(file("prometheus"))
  .settings(commonSettings: _*)
  .settings(name := "filodb-prometheus")
  .settings(libraryDependencies ++= promDeps)
  .dependsOn(core % "compile->compile; test->test")
  .dependsOn(query % "compile->compile; test->test")

lazy val query = project
  .in(file("query"))
  .settings(commonSettings: _*)
  .settings(name := "filodb-query")
  .dependsOn(core % "compile->compile; test->test")

lazy val cassandra = project
  .in(file("cassandra"))
  .settings(commonSettings: _*)
  .settings(name := "filodb-cassandra")
  .settings(libraryDependencies ++= cassDeps)
  .dependsOn(core % "compile->compile; test->test", coordinator)

lazy val cli = project
  .in(file("cli"))
  .settings(commonSettings: _*)
  .settings(name := "filodb-cli")
  .settings(libraryDependencies ++= cliDeps)
  .settings(cliAssemblySettings: _*)
  .dependsOn(prometheus % "compile->compile; test->test")
  .dependsOn(core, coordinator  % "test->test", cassandra)

lazy val kafka = project
  .in(file("kafka"))
  .settings(name := "filodb-kafka")
  .settings(commonSettings: _*)
  .settings(kafkaSettings: _*)
  .settings(itSettings: _*)
  .settings(assemblySettings: _*)
  .settings(libraryDependencies ++= kafkaDeps)
  .dependsOn(
    core % "compile->compile; it->test",
    coordinator % "compile->compile; test->test")
  .configs(IntegrationTest, MultiJvm)

lazy val bootstrapper = project
  .in(file("akka-bootstrapper"))
  .settings(commonSettings: _*)
  .settings(multiJvmMaybeSettings: _*)
  .settings(name := "akka-bootstrapper")
  .settings(libraryDependencies ++= bootstrapperDeps)
  .configs(MultiJvm)

lazy val http = project
  .in(file("http"))
  .settings(commonSettings: _*)
  .settings(name := "http")
  .settings(libraryDependencies ++= httpDeps)
  .dependsOn(core, coordinator % "compile->compile; test->test")

lazy val standalone = project
  .in(file("standalone"))
  .settings(commonSettings: _*)
  .settings(multiJvmMaybeSettings: _*)
  .settings(assemblySettings: _*)
  .settings(libraryDependencies ++= standaloneDeps)
  .dependsOn(core, prometheus % "test->test", coordinator % "compile->compile; test->test",
    cassandra, kafka, http, bootstrapper, tsgenerator % Test)
  .configs(MultiJvm)

lazy val spark = project
  .in(file("spark"))
  .settings(name := "filodb-spark")
  .settings(commonSettings: _*)
  .settings(libraryDependencies ++= sparkDeps)
  .settings(itSettings: _*)
  .settings(jvmPerTestSettings: _*)
  .settings(assemblyExcludeScala: _*)
  // Disable tests for now since lots of work remaining to enable Spark
  .settings(test := {})
  .dependsOn(core % "compile->compile; test->test; it->test",
    coordinator % "compile->compile; test->test",
    cassandra % "compile->compile; test->test; it->test")
  .configs( IntegrationTest )

lazy val jmh = project
  .in(file("jmh"))
  .settings(commonSettings: _*)
  .settings(name := "filodb-jmh")
  .settings(libraryDependencies ++= jmhDeps)
  .settings(publish := {})
  .enablePlugins(JmhPlugin)
  .dependsOn(core % "compile->compile; compile->test", spark, tsgenerator)

lazy val stress = project
  .in(file("stress"))
  .settings(commonSettings: _*)
  .settings(name := "filodb-stress")
  .settings(libraryDependencies ++= stressDeps)
  .settings(assemblyExcludeScala: _*)
  .dependsOn(spark)

lazy val tsgenerator = project
  .in(file("tsgenerator"))
  .settings(commonSettings: _*)
  .settings(name := "tsgenerator")
  .settings(libraryDependencies ++= tsgeneratorDeps)
  .settings(tsgeneratorAssemblySettings: _*)
  .dependsOn(coordinator, prometheus)

// Zookeeper pulls in slf4j-log4j12 which we DON'T want
val excludeZK = ExclusionRule(organization = "org.apache.zookeeper")
// This one is brought by Spark by default
val excludeSlf4jLog4j = ExclusionRule(organization = "org.slf4j", name = "slf4j-log4j12")
val excludeJersey = ExclusionRule(organization = "com.sun.jersey")
// The default minlog only logs to STDOUT.  We want to log to SLF4J.
val excludeMinlog = ExclusionRule(organization = "com.esotericsoftware", name = "minlog")
val excludeOldLz4 = ExclusionRule(organization = "net.jpountz.lz4", name = "lz4")


/* Versions in various modules versus one area of build */
val akkaVersion       = "2.5.13" // akka-http/akka-stream compat. TODO when kamon-akka-remote is akka 2.5.4 compat
val akkaHttpVersion   = "10.1.3"
val cassDriverVersion = "3.0.2"
val ficusVersion      = "1.1.2"
val kamonVersion      = "1.1.3"
val monixKafkaVersion = "0.15"
val sparkVersion      = "2.0.0"

/* Dependencies shared */
val logbackDep        = "ch.qos.logback"             % "logback-classic"       % "1.2.3"
val log4jDep          = "log4j"                      % "log4j"                 % "1.2.17"
val scalaLoggingDep   = "com.typesafe.scala-logging" %% "scala-logging"        % "3.7.2"
val scalaTest         = "org.scalatest"              %% "scalatest"            % "2.2.6" // TODO upgrade to 3.0.4
val scalaCheck        = "org.scalacheck"             %% "scalacheck"           % "1.11.0"
val akkaHttp          = "com.typesafe.akka"          %% "akka-http"            % akkaHttpVersion
val akkaHttpTestkit   = "com.typesafe.akka"          %% "akka-http-testkit"    % akkaHttpVersion
val akkaHttpCirce     = "de.heikoseeberger"          %% "akka-http-circe"      % "1.21.0"
val circeGeneric      = "io.circe"                   %% "circe-generic"        % "0.8.0"
val circeParser       = "io.circe"                   %% "circe-parser"         % "0.8.0"

lazy val commonDeps = Seq(
  "io.kamon" %% "kamon-core" % kamonVersion,
  "io.kamon" %% "kamon-akka-2.5" % "1.0.1",
  "io.kamon" %% "kamon-executors" % "1.0.1",
  "io.kamon" %% "kamon-akka-remote-2.5" % "1.0.1",
  logbackDep % Test,
  scalaTest  % Test,
  scalaCheck % "test"
)

lazy val scalaxyDep = "com.nativelibs4java"  %% "scalaxy-loops"     % "0.3.3" % "provided"

lazy val memoryDeps = commonDeps ++ Seq(
  "com.github.jnr"       %  "jnr-ffi"          % "2.1.6",
  "joda-time"            % "joda-time"         % "2.2",
  "org.joda"             % "joda-convert"      % "1.2",
  "org.lz4"              %  "lz4-java"         % "1.4",
  "org.jctools"          % "jctools-core"      % "2.0.1",
  "org.spire-math"      %% "debox"             % "0.8.0",
  scalaLoggingDep,
  scalaxyDep
)

lazy val coreDeps = commonDeps ++ Seq(
  scalaLoggingDep,
  "org.slf4j"             % "slf4j-api"         % "1.7.10",
  "com.beachape"         %% "enumeratum"        % "1.5.10",
  "io.monix"             %% "monix"             % "2.3.0",
  "com.googlecode.concurrentlinkedhashmap" % "concurrentlinkedhashmap-lru" % "1.4",
  "net.ceedubs"          %% "ficus"             % ficusVersion,
  "io.fastjson"           % "boon"              % "0.33",
  "com.googlecode.javaewah" % "JavaEWAH"        % "1.1.6",
  "com.github.rholder.fauxflake" % "fauxflake-core" % "1.1.0",
  "org.scalactic"        %% "scalactic"         % "2.2.6",
  "org.apache.lucene"     % "lucene-core"       % "7.3.0",
  scalaxyDep
)

lazy val cassDeps = commonDeps ++ Seq(
  // other dependencies separated by commas
  "org.lz4"                %  "lz4-java"         % "1.4",
  "com.datastax.cassandra" % "cassandra-driver-core" % cassDriverVersion,
  logbackDep % Test
)

lazy val coordDeps = commonDeps ++ Seq(
  "com.typesafe.akka"    %% "akka-slf4j"        % akkaVersion,
  "com.typesafe.akka"    %% "akka-cluster"      % akkaVersion,
  "com.github.romix.akka" %% "akka-kryo-serialization" % "0.5.0" excludeAll(excludeMinlog, excludeOldLz4),
  // Redirect minlog logs to SLF4J
   "com.dorkbox"         % "MinLog-SLF4J"       % "1.12",
  "com.opencsv"          % "opencsv"            % "3.3",
  "com.typesafe.akka"    %% "akka-testkit"      % akkaVersion % Test,
  "com.typesafe.akka"    %% "akka-multi-node-testkit" % akkaVersion % Test
)

lazy val cliDeps = Seq(
  logbackDep,
  "io.kamon" %% "kamon-akka-2.5" % "1.0.1",
  "io.kamon" %% "kamon-executors" % "1.0.1",
  "io.kamon" %% "kamon-akka-remote-2.5" % "1.0.1",
  "com.quantifind"    %% "sumac"          % "0.3.0"
)

lazy val kafkaDeps = Seq(
  "io.monix"          %% "monix-kafka-1x" % monixKafkaVersion,
  "org.apache.kafka"  % "kafka-clients"   % "1.0.0" % "compile,test" exclude("org.slf4j", "slf4j-log4j12"),
  "com.typesafe.akka" %% "akka-testkit"   % akkaVersion % "test,it",
  scalaTest  % "test,it",
  logbackDep % "test,it")

lazy val promDeps = Seq(
  "com.google.protobuf" % "protobuf-java" % "2.5.0"
)

lazy val tsgeneratorDeps = Seq(
  logbackDep,
  "io.monix" %% "monix-kafka-1x" % monixKafkaVersion,
  "org.rogach" %% "scallop" % "3.1.1"
)

lazy val httpDeps = Seq(
  logbackDep,
  akkaHttp,
  akkaHttpCirce,
  circeGeneric,
  circeParser,
  akkaHttpTestkit % Test
)

lazy val standaloneDeps = Seq(
  logbackDep,
  "io.kamon" %% "kamon-zipkin" % "1.0.0",
  "net.ceedubs"          %% "ficus"             % ficusVersion      % Test,
  "com.typesafe.akka"    %% "akka-multi-node-testkit" % akkaVersion % Test
)

lazy val bootstrapperDeps = Seq(
  logbackDep,
  scalaLoggingDep,
  "com.typesafe.akka"            %% "akka-cluster"            % akkaVersion,
  // akka http should be a compile time dependency only. Users of this library may want to use a different http server
  akkaHttp          % "test; provided",
  akkaHttpCirce     % "test; provided",
  circeGeneric      % "test; provided",
  circeParser       % "test; provided",
  "com.typesafe.akka"            %% "akka-slf4j"              % akkaVersion,
  "dnsjava"                      %  "dnsjava"                 % "2.1.8",
  "org.scalaj"                   %% "scalaj-http"             % "2.3.0",
  "com.typesafe.akka"            %% "akka-testkit"            % akkaVersion   % Test,
  "com.typesafe.akka"            %% "akka-multi-node-testkit" % akkaVersion   % Test,
  scalaTest   % Test
)

lazy val sparkDeps = Seq(
  // We don't want LOG4J.  We want Logback!  The excludeZK is to help with a conflict re Coursier plugin.
  "org.apache.spark" %% "spark-hive"              % sparkVersion % "provided" excludeAll(excludeSlf4jLog4j, excludeZK),
  "org.apache.spark" %% "spark-hive-thriftserver" % sparkVersion % "provided" excludeAll(excludeSlf4jLog4j, excludeZK),
  "org.apache.spark" %% "spark-streaming"         % sparkVersion % "provided",
  scalaTest % "it"
)

lazy val jmhDeps = Seq(
  scalaxyDep,
  "org.apache.spark" %% "spark-sql" % sparkVersion excludeAll(excludeSlf4jLog4j, excludeZK, excludeJersey)
)

lazy val stressDeps = Seq(
  "com.databricks"       %% "spark-csv"         % "1.3.0",
  scalaxyDep,
  "org.apache.spark"     %% "spark-sql"         % sparkVersion % "provided" excludeAll(excludeZK),
  "org.apache.spark"     %% "spark-streaming"   % sparkVersion % "provided" excludeAll(excludeZK)
)

/* Settings */

/* The REPL can’t cope with -Ywarn-unused:imports or -Xfatal-warnings
   so we disable for console */
lazy val consoleSettings = Seq(
 scalacOptions in (Compile, console) ~= (_.filterNot(Set(
   "-Ywarn-unused-import",
   "-Xfatal-warnings"))),
 scalacOptions in (Test, console) ~= (_.filterNot(Set(
   "-Ywarn-unused-import",
   "-Xfatal-warnings"))))

lazy val compilerSettings = Seq(
  autoAPIMappings := true,
  scalacOptions ++= Seq(
    "-deprecation",
    "-encoding", "UTF-8",
    "-unchecked",
    "-feature",
    "-Xfuture",
    "-Xcheckinit",
    "-Xfatal-warnings",
    "-Ywarn-inaccessible",
    "-Ywarn-dead-code",
    "-Ywarn-unused-import",
    "-Yno-adapted-args",
    "-language:existentials",
    "-language:experimental.macros",
    "-language:higherKinds",
    "-language:implicitConversions"
    // TODO relocate here: -Ywarn-unused-import, add -Ywarn-numeric-widen
    // TODO in 2.12: remove: -Yinline-warnings, add the new applicable ones
  ),

  javacOptions ++= Seq(
    "-encoding", "UTF-8"
  ))

// Create a default Scala style task to run with tests
lazy val testScalastyle = taskKey[Unit]("testScalastyle")

lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")

lazy val styleSettings = Seq(
  scalastyleFailOnError := true,
  testScalastyle := org.scalastyle.sbt.ScalastylePlugin.scalastyle.in(Test).toTask("").value,
  // (scalastyleConfig in Test) := "scalastyle-test-config.xml",
  // This is disabled for now, cannot get ScalaStyle to recognize the file above for some reason :/
  // (test in Test) <<= (test in Test) dependsOn testScalastyle,
  compileScalastyle := org.scalastyle.sbt.ScalastylePlugin.scalastyle.in(Compile).toTask("").value,
  // Is running this on compile too much?
  (compile in Test) := ((compile in Test) dependsOn compileScalastyle).value)

lazy val evictionSettings = Seq(
  evictionWarningOptions in update := EvictionWarningOptions.default
    .withWarnTransitiveEvictions(false)
    .withWarnDirectEvictions(false)
    .withWarnScalaVersionEviction(false))

// TODO disabled for now: "-Xlint:infer-any", "-Xlint",
lazy val lintSettings = Seq(
  scalacOptions ++= Seq(
    "-Xlint:adapted-args",
    "-Xlint:nullary-unit",
    "-Xlint:inaccessible",
    "-Xlint:nullary-override",
    "-Xlint:missing-interpolator",
    "-Xlint:doc-detached",
    "-Xlint:private-shadow",
    "-Xlint:type-parameter-shadow",
    "-Xlint:poly-implicit-overload",
    "-Xlint:option-implicit",
    "-Xlint:delayedinit-select",
    "-Xlint:by-name-right-associative",
    "-Xlint:package-object-classes",
    "-Xlint:unsound-match",
    "-Xlint:stars-align"
  ),

  javacOptions ++= Seq(
    "-Xlint",
    "-Xlint:deprecation",
    "-Xlint:unchecked"
  ))

lazy val disciplineSettings =
  compilerSettings ++
    lintSettings ++
    styleSettings ++
    evictionSettings ++
    consoleSettings

lazy val testSettings = Seq(
  parallelExecution in Test := false,
  fork in Test := true,
  testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oF"),
  // Uncomment below to debug Typesafe Config file loading
  // javaOptions ++= List("-Xmx2G", "-Dconfig.trace=loads"),
  // Make Akka tests more resilient esp for CI/CD/Travis/etc.
  javaOptions ++= List("-Xmx2G", "-Dakka.test.timefactor=3"),
  // Needed to avoid cryptic EOFException crashes in forked tests
  // in Travis with `sudo: false`.
  // See https://github.com/sbt/sbt/issues/653
  // and https://github.com/travis-ci/travis-ci/issues/3775
  concurrentRestrictions in Global := Seq(
    // Tags.limit(Tags.CPU, java.lang.Runtime.getRuntime().availableProcessors()),
    Tags.limit(Tags.CPU, 1),
    // limit to 1 concurrent test task, even across sub-projects
    Tags.limit(Tags.Test, 1),
    // Note: some components of tests seem to have the "Untagged" tag rather than "Test" tag.
    // So, we limit the sum of "Test", "Untagged" tags to 1 concurrent
    Tags.limitSum(1, Tags.Test, Tags.Untagged))
)

lazy val itSettings = Defaults.itSettings ++ Seq(
  fork in IntegrationTest := true,

  parallelExecution in IntegrationTest := false,

  internalDependencyClasspath in IntegrationTest := (Classpaths.concat(
    internalDependencyClasspath in IntegrationTest, exportedProducts in Test)).value)

lazy val multiJvmSettings = SbtMultiJvm.multiJvmSettings ++ Seq(
  compile in MultiJvm := ((compile in MultiJvm) triggeredBy (compile in Test)).value)

lazy val testMultiJvmToo = Seq(
  // make sure that MultiJvm tests are executed by the default test target,
  // and combine the results from ordinary test and multi-jvm tests
  executeTests in Test := {
    val testResults = (executeTests in Test).value
    val multiNodeResults = (executeTests in MultiJvm).value
    val overall =
      if (testResults.overall.id < multiNodeResults.overall.id)
        multiNodeResults.overall
      else
        testResults.overall
    Tests.Output(overall,
      testResults.events ++ multiNodeResults.events,
      testResults.summaries ++ multiNodeResults.summaries)
  }
)

lazy val multiJvmMaybeSettings = multiJvmSettings ++ {
                                 if (sys.env.contains("MAYBE_MULTI_JVM")) testMultiJvmToo else Nil }

// Fork a separate JVM for each test, instead of one for all tests in a module.
// This is necessary for Spark tests due to initialization, for example
lazy val jvmPerTestSettings = {
  def jvmPerTest(tests: Seq[TestDefinition]) =
    tests map { test =>
      Tests.Group(
        name = test.name,
        tests = Seq(test),
        runPolicy = Tests.SubProcess(ForkOptions(runJVMOptions = Seq.empty[String])))
    }

  Seq(testGrouping in Test := ((definedTests in Test) map jvmPerTest).value)
}

// NOTE: The -Xms1g and using RemoteActorRefProvider (no Cluster startup) both help CLI startup times
lazy val shellScript = """#!/bin/bash
# ClusterActorRefProvider by default. Enable this line if needed for some of the commands
# allprops="-Dakka.actor.provider=akka.remote.RemoteActorRefProvider"
while [ "${1:0:2}" = "-D" ]
do
  allprops="$allprops $1"
  shift
done
if [ ! -z "$JAVA_HOME" ]; then
  CMD="$JAVA_HOME/bin/java"
else
  CMD="java"
fi
if [ ! -z "$FILO_CONFIG_FILE" ]; then
  config="-Dconfig.file=$FILO_CONFIG_FILE"
fi
: ${FILOLOG:="."}
exec $CMD -Xmx4g -Xms1g -DLOG_DIR=$FILOLOG $config $allprops -jar "$0" "$@"  ;
""".split("\n")

lazy val kafkaSettings = Seq(

  aggregate in update := false,

  updateOptions := updateOptions.value.withCachedResolution(true))

// Create a new MergeStrategy for aop.xml files
// Needed for Kamon.io async / Akka tracing / AspectJ weaving
val aopMerge = new sbtassembly.MergeStrategy {
  val name = "aopMerge"
  import scala.xml._
  import scala.xml.dtd._

  def apply(tempDir: File, path: String, files: Seq[File]): Either[String, Seq[(File, String)]] = {
    val dt = DocType("aspectj", PublicID("-//AspectJ//DTD//EN", "http://www.eclipse.org/aspectj/dtd/aspectj.dtd"), Nil)
    val file = MergeStrategy.createMergeTarget(tempDir, path)
    val xmls: Seq[Elem] = files.map(XML.loadFile)
    val aspectsChildren: Seq[Node] = xmls.flatMap(_ \\ "aspectj" \ "aspects" \ "_")
    val weaverChildren: Seq[Node] = xmls.flatMap(_ \\ "aspectj" \ "weaver" \ "_")
    val options: String = xmls.map(x => (x \\ "aspectj" \ "weaver" \ "@options").text).mkString(" ").trim
    val weaverAttr = if (options.isEmpty) Null else new UnprefixedAttribute("options", options, Null)
    val aspects = new Elem(null, "aspects", Null, TopScope, false, aspectsChildren: _*)
    val weaver = new Elem(null, "weaver", weaverAttr, TopScope, false, weaverChildren: _*)
    val aspectj = new Elem(null, "aspectj", Null, TopScope, false, aspects, weaver)
    XML.save(file.toString, aspectj, "UTF-8", xmlDecl = false, dt)
    IO.append(file, IO.Newline.getBytes(IO.defaultCharset))
    Right(Seq(file -> path))
  }
}

lazy val assemblySettings = Seq(
  assemblyMergeStrategy in assembly := {
    case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
    case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
    case m if m.toLowerCase.matches("meta-inf.*\\.properties") => MergeStrategy.discard
    case PathList("META-INF", "aop.xml") => aopMerge
    case PathList(ps @ _*) if ps.last endsWith ".txt.1" => MergeStrategy.first
    case "reference.conf"    => MergeStrategy.concat
    case "application.conf"  => MergeStrategy.concat
    case "filodb-defaults.conf"  => MergeStrategy.concat
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  },
  assemblyShadeRules in assembly := Seq(
    ShadeRule.rename("com.datastax.driver.**" -> "filodb.datastax.driver.@1").inAll,
    ShadeRule.rename("com.google.common.**" -> "filodb.com.google.common.@1").inAll,
    ShadeRule.rename("org.apache.http.**" -> "filodb.org.apache.http.@1").inAll,
    ShadeRule.rename("com.google.guava.**" -> "filodb.com.google.guava.@1").inAll
  ),
  test in assembly := {} //noisy for end-user since the jar is not available and user needs to build the project locally
)

lazy val assemblyExcludeScala = assemblySettings ++ Seq(
  assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false))

// Builds cli as a standalone executable to make it easier to launch commands
lazy val cliAssemblySettings = assemblySettings ++ Seq(
  assemblyOption in assembly := (assemblyOption in assembly).value.copy(
    prependShellScript = Some(shellScript)),
  assemblyJarName in assembly := s"filo-cli-${version.value}"
)

// builds timeseries-gen as a fat jar so it can be executed for development test scenarios
lazy val tsgeneratorAssemblySettings = assemblySettings ++ Seq(
  assemblyJarName in assembly := s"tsgenerator-${version.value}"
)

lazy val publishSettings = Seq(
  organizationName := "FiloDB",
  publishMavenStyle := true,
  publishArtifact in Test := false,
  publishArtifact in IntegrationTest := false,
  licenses += ("Apache-2.0", url("http://choosealicense.com/licenses/apache/")),
  pomIncludeRepository := { x => false }
)

lazy val moduleSettings = Seq(
  resolvers ++= Seq(
    "Velvia Bintray" at "https://dl.bintray.com/velvia/maven",
    "spray repo" at "http://repo.spray.io"
  ),

  cancelable in Global := true,

  incOptions := incOptions.value.withNameHashing(true),

  ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) })

lazy val commonSettings =
  buildSettings ++
    disciplineSettings ++
    moduleSettings ++
    testSettings ++
    publishSettings