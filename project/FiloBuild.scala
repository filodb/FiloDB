import sbt._
import Keys._

import com.typesafe.sbt.SbtMultiJvm
import com.typesafe.sbt.SbtMultiJvm.MultiJvmKeys.MultiJvm
import org.scalastyle.sbt.ScalastylePlugin
import pl.project13.scala.sbt.JmhPlugin
import sbtassembly.AssemblyPlugin.autoImport._

/**
 * FiloDB modules and dependencies
 */
object FiloBuild extends Build {
  import FiloSettings._

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
    .dependsOn(prometheus % "compile->compile; test->test")
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
    .settings(libraryDependencies ++= queryDeps)
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
      cassandra, kafka, http, bootstrapper, gateway % Test)
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
    .dependsOn(core % "compile->compile; compile->test", spark, gateway)

  lazy val stress = project
    .in(file("stress"))
    .settings(commonSettings: _*)
    .settings(name := "filodb-stress")
    .settings(libraryDependencies ++= stressDeps)
    .settings(assemblyExcludeScala: _*)
    .dependsOn(spark)

  lazy val gateway = project
    .in(file("gateway"))
    .settings(commonSettings: _*)
    .settings(name := "filodb-gateway")
    .settings(libraryDependencies ++= gatewayDeps)
    .settings(gatewayAssemblySettings: _*)
    .dependsOn(coordinator % "compile->compile; test->test",
               prometheus)

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
  val sttpVersion       = "1.3.3"

  /* Dependencies shared */
  val logbackDep        = "ch.qos.logback"             % "logback-classic"       % "1.2.3"
  val log4jDep          = "log4j"                      % "log4j"                 % "1.2.17"
  val scalaLoggingDep   = "com.typesafe.scala-logging" %% "scala-logging"        % "3.7.2"
  val scalaTest         = "org.scalatest"              %% "scalatest"            % "2.2.6" // TODO upgrade to 3.0.4
  val scalaCheck        = "org.scalacheck"             %% "scalacheck"           % "1.11.0"
  val akkaHttp          = "com.typesafe.akka"          %% "akka-http"            % akkaHttpVersion withJavadoc()
  val akkaHttpTestkit   = "com.typesafe.akka"          %% "akka-http-testkit"    % akkaHttpVersion withJavadoc()
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
    "joda-time"            % "joda-time"         % "2.2" withJavadoc(),
    "org.joda"             % "joda-convert"      % "1.2",
    "org.lz4"              %  "lz4-java"         % "1.4",
    "org.jctools"          % "jctools-core"      % "2.0.1" withJavadoc(),
    "org.spire-math"      %% "debox"             % "0.8.0" withJavadoc(),
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
    "com.googlecode.javaewah" % "JavaEWAH"        % "1.1.6" withJavadoc(),
    "com.github.rholder.fauxflake" % "fauxflake-core" % "1.1.0",
    "org.scalactic"        %% "scalactic"         % "2.2.6" withJavadoc(),
    "org.apache.lucene"     % "lucene-core"       % "7.3.0" withJavadoc(),
    scalaxyDep
  )

  lazy val cassDeps = commonDeps ++ Seq(
    // other dependencies separated by commas
    "org.lz4"                %  "lz4-java"         % "1.4",
    "com.datastax.cassandra" % "cassandra-driver-core" % cassDriverVersion,
    logbackDep % Test
  )

  lazy val queryDeps = commonDeps ++ Seq(
    "com.tdunning"         % "t-digest"           % "3.1"
  )

  lazy val coordDeps = commonDeps ++ Seq(
    "com.typesafe.akka"    %% "akka-slf4j"        % akkaVersion,
    "com.typesafe.akka"    %% "akka-cluster"      % akkaVersion withJavadoc(),
    "com.github.romix.akka" %% "akka-kryo-serialization" % "0.5.0" excludeAll(excludeMinlog, excludeOldLz4),
    "de.javakaffee"        % "kryo-serializers"      % "0.42" excludeAll(excludeMinlog),
    "io.kamon"             %% "kamon-prometheus"  % "1.1.1",
    // Redirect minlog logs to SLF4J
     "com.dorkbox"         % "MinLog-SLF4J"       % "1.12",
    "com.opencsv"          % "opencsv"            % "3.3",
    "com.github.TanUkkii007" %% "akka-cluster-custom-downing" % "0.0.12",
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

  lazy val gatewayDeps = commonDeps ++ Seq(
    scalaxyDep,
    "io.monix" %% "monix-kafka-1x" % monixKafkaVersion,
    "org.rogach" %% "scallop" % "3.1.1"
  )

  lazy val httpDeps = Seq(
    logbackDep,
    akkaHttp,
    akkaHttpCirce,
    circeGeneric,
    circeParser,
    akkaHttpTestkit % Test,
    "org.xerial.snappy" % "snappy-java" % "1.1.7.2"
  )

  lazy val standaloneDeps = Seq(
    logbackDep,
    "io.kamon"              %% "kamon-zipkin"            % "1.0.0",
    "net.ceedubs"           %% "ficus"                   % ficusVersion      % Test,
    "com.typesafe.akka"     %% "akka-multi-node-testkit" % akkaVersion       % Test,
    "com.softwaremill.sttp" %% "circe"                   % sttpVersion       % Test,
    "com.softwaremill.sttp" %% "akka-http-backend"       % sttpVersion       % Test,
    "com.softwaremill.sttp" %% "core"                    % sttpVersion       % Test,
    "com.typesafe.akka"     %% "akka-stream"             % "2.5.11"          % Test
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
}


