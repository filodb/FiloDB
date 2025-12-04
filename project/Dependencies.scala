import sbt._

object Dependencies {

  // Zookeeper pulls in slf4j-log4j12 which we DON'T want
  val excludeZK = ExclusionRule(organization = "org.apache.zookeeper")
  // This one is brought by Spark by default
  val excludeSlf4jLog4j = ExclusionRule(organization = "org.slf4j", name = "slf4j-log4j12")
  val excludeJersey = ExclusionRule(organization = "com.sun.jersey")
  // The default minlog only logs to STDOUT.  We want to log to SLF4J.
  val excludeMinlog = ExclusionRule(organization = "com.esotericsoftware", name = "minlog")
  val excludeKryoShaded = ExclusionRule(organization = "com.esotericsoftware", name = "kryo-shaded")
  val excludeOldLz4 = ExclusionRule(organization = "net.jpountz.lz4", name = "lz4")
  val excludeNetty  = ExclusionRule(organization = "io.netty", name = "netty-handler")
  val excludeXBean = ExclusionRule(organization = "org.apache.xbean", name = "xbean-asm6-shaded")
  val excludegrpc = ExclusionRule(organization = "io.grpc")
  val excludeAkka = ExclusionRule(organization = "com.typesafe.akka")
  val excludeOkHttp3 = ExclusionRule(organization = "com.squareup.okhttp3", name = "okhttp")
  // Exclude Scala 2.12 transitive dependencies to prevent cross-version conflicts
  val excludeSpire212 = ExclusionRule(organization = "org.typelevel", name = "spire_2.12")
  val excludeCatsKernel212 = ExclusionRule(organization = "org.typelevel", name = "cats-kernel_2.12")
  val excludeScalaTest212 = ExclusionRule(organization = "org.scalatest", name = "scalatest_2.12")
  val excludeScalaCheck212 = ExclusionRule(organization = "org.scalacheck", name = "scalacheck_2.12")

  /* Versions in various modules versus one area of build */
  val akkaVersion       = "2.6.21" // Upgraded for Scala 2.13 and JDK 17 support
  val akkaHttpVersion   = "10.2.10" // Compatible with Akka 2.6.21
  val cassDriverVersion = "3.7.1"
  val ficusVersion      = "1.5.2" // Updated for Scala 2.13
  val kamonBundleVersion = "2.7.3"
  val otelVersion       = "1.54.1"
  val otelInstVersion   = "2.20.1-alpha"
  val monixKafkaVersion = "1.0.0-RC7" // Updated for Scala 2.13
  val sparkVersion      = "3.5.7" // Upgraded for Scala 2.13 support
  val sttpVersion       = "1.7.2" // Updated for Scala 2.13

  /* Dependencies shared */
  val logbackDep        = "ch.qos.logback"             % "logback-classic"       % "1.5.6"
  val log4jDep          = "log4j"                      % "log4j"                 % "1.2.17"
  val scalaLoggingDep   = "com.typesafe.scala-logging" %% "scala-logging"        % "3.9.5"  // Updated for Scala 2.13
  val scalaTest         = "org.scalatest"              %% "scalatest"            % "3.2.18"  // Updated for Scala 2.13
  val scalaCheck        = "org.scalacheck"             %% "scalacheck"           % "1.17.0"  // Updated for Scala 2.13
  val scalaTestPlus     = "org.scalatestplus"          %% "scalacheck-1-17"      % "3.2.18.0"  // Updated for Scala 2.13
  val akkaHttp          = "com.typesafe.akka"          %% "akka-http"            % akkaHttpVersion withJavadoc()
  val akkaHttpTestkit   = "com.typesafe.akka"          %% "akka-http-testkit"    % akkaHttpVersion withJavadoc()
  val akkaHttpCirce     = "de.heikoseeberger"          %% "akka-http-circe"      % "1.31.0" // Compatible with circe 0.12.x and Akka HTTP 10.2
  val circeGeneric      = "io.circe"                   %% "circe-generic"        % "0.12.3" // Downgraded to match sttp 1.7.2
  val circeParser       = "io.circe"                   %% "circe-parser"         % "0.12.3" // Downgraded to match sttp 1.7.2

  lazy val commonDeps = Seq(
    "io.kamon" %% "kamon-bundle"  % kamonBundleVersion,
    "io.kamon" %% "kamon-testkit" % kamonBundleVersion % Test,
    "io.opentelemetry"             % "opentelemetry-api"                    % otelVersion,
    "io.opentelemetry"             % "opentelemetry-sdk-metrics"            % otelVersion,
    "io.opentelemetry"             % "opentelemetry-exporter-otlp"          % otelVersion,
    "io.opentelemetry"             % "opentelemetry-exporter-logging-otlp"  % otelVersion,
    "io.opentelemetry.instrumentation" % "opentelemetry-runtime-telemetry-java8" % otelInstVersion,
    "io.opentelemetry.instrumentation" % "opentelemetry-oshi"                    % otelInstVersion,
    "org.scala-lang.modules" %% "scala-collection-compat" % "2.11.0",  // Scala 2.13 collections compatibility
    logbackDep % Test,
    scalaTest  % Test,
    "com.softwaremill.quicklens" %% "quicklens" % "1.9.7" % Test, // Updated for Scala 2.13
    "org.apache.xbean" % "xbean-asm6-shaded" % "4.10" % Test,
    scalaCheck % Test,
    scalaTestPlus % Test
  )

  lazy val memoryDeps = commonDeps ++ Seq(
    "com.github.jnr"       %  "jnr-ffi"          % "2.2.12",
    "joda-time"            % "joda-time"         % "2.2" withJavadoc(),
    "org.joda"             % "joda-convert"      % "1.2",
    "org.lz4"              %  "lz4-java"         % "1.4",
    "org.agrona"           %  "agrona"           % "0.9.35",
    "org.jctools"          % "jctools-core"      % "4.0.3" withJavadoc(),
    scalaLoggingDep
  )

  lazy val coreDeps = commonDeps ++ Seq(
    scalaLoggingDep,
    "org.slf4j"                    % "slf4j-api"          % "1.7.10",
    "com.beachape"                 %% "enumeratum"        % "1.5.13",  // Updated for Scala 2.13 (1.5.10 not available)
    "io.monix"                     %% "monix"             % "3.4.0",
    "org.typelevel"                %% "spire"             % "0.18.0",  // Fast numeric operations
    "com.googlecode.concurrentlinkedhashmap"              % "concurrentlinkedhashmap-lru" % "1.4",
    "com.iheart"                   %% "ficus"             % ficusVersion,
    "io.fastjson"                  % "boon"               % "0.33",
    "com.googlecode.javaewah"      % "JavaEWAH"           % "1.1.6" withJavadoc(),
    "com.github.rholder.fauxflake" % "fauxflake-core"     % "1.1.0",
    "org.scalactic"                %% "scalactic"         % "3.2.0" withJavadoc(),
    "org.apache.lucene"            % "lucene-core"        % "9.7.0" withJavadoc(),
    "org.apache.lucene"            % "lucene-facet"       % "9.7.0" withJavadoc(),
    "com.github.alexandrnikitin"   %% "bloom-filter"      % "0.13.1",  // Updated for Scala 2.13
    "org.rocksdb"                  % "rocksdbjni"         % "6.29.5",
    "com.esotericsoftware"         % "kryo"               % "5.5.0" excludeAll(excludeMinlog, excludeKryoShaded),
    "com.dorkbox"                  % "MinLog-SLF4J"       % "1.12",
    "com.github.ben-manes.caffeine" % "caffeine"          % "3.0.5",
    "com.twitter"                  %% "chill"             % "0.10.0",  // Updated for Scala 2.13
    "org.apache.commons"           % "commons-lang3"      % "3.14.0"
  )

  lazy val sparkJobsDeps = commonDeps ++ Seq(
    "org.apache.spark"       %%      "spark-core" % sparkVersion % Provided,
    "org.apache.spark"       %%      "spark-sql"  % sparkVersion % Provided,
    "org.apache.spark"       %%      "spark-core" % sparkVersion % Test excludeAll(excludeNetty, excludeXBean),
    "org.apache.spark"       %%      "spark-sql"  % sparkVersion % Test excludeAll(excludeNetty)
  )

  lazy val cassDeps = commonDeps ++ Seq(
    // other dependencies separated by commas
    "org.lz4"                %  "lz4-java"             % "1.4",
    "com.datastax.cassandra" % "cassandra-driver-core" % cassDriverVersion,
    logbackDep % Test
  )

  lazy val queryDeps = commonDeps ++ Seq(
    "com.typesafe.akka"       %% "akka-actor"                           % akkaVersion,
    "com.tdunning"            % "t-digest"                              % "3.1",
    "com.softwaremill.sttp"   %% "circe"                                % sttpVersion ,
    "com.softwaremill.sttp"   %% "async-http-client-backend-future"     % sttpVersion,
    "com.softwaremill.sttp"   %% "core"                                 % sttpVersion,
    "org.apache.datasketches" % "datasketches-java"                     % "3.0.0",
    circeGeneric
  )

  lazy val coordDeps = commonDeps ++ Seq(
    "com.typesafe.akka"      %% "akka-slf4j"                  % akkaVersion,
    "com.typesafe.akka"      %% "akka-cluster"                % akkaVersion withJavadoc(),
    "com.typesafe.akka"      %% "akka-cluster-tools"          % akkaVersion, // For ClusterSingleton support
    "io.altoo"               %% "akka-kryo-serialization"     % "2.4.3" excludeAll(excludeMinlog, excludeOldLz4, excludeAkka, excludeKryoShaded), // Updated for Akka 2.6 and Scala 2.13
    "de.javakaffee"          % "kryo-serializers"             % "0.45" excludeAll(excludeMinlog, excludeAkka, excludeKryoShaded), // Updated version
    "io.kamon"               %% "kamon-prometheus"            % kamonBundleVersion  excludeAll(excludeOkHttp3),
    // Redirect minlog logs to SLF4J
    "com.dorkbox"            % "MinLog-SLF4J"                 % "1.12",
    "com.opencsv"            % "opencsv"                      % "3.3",
    "org.sisioh"             %% "akka-cluster-custom-downing" % "0.1.0" excludeAll(excludeAkka), // Updated to 0.1.0 for Akka 2.6.4 compatibility
    "com.typesafe.akka"      %% "akka-testkit"                % akkaVersion % Test,
    "com.typesafe.akka"      %% "akka-multi-node-testkit"     % akkaVersion % Test,
    "org.apache.commons" % "commons-text" % "1.9"
  )

  lazy val cliDeps = Seq(
    logbackDep,
    "io.kamon"          %% "kamon-bundle"        % kamonBundleVersion,
    "org.rogach"        %% "scallop"             % "3.3.0"  // Updated for Scala 2.13
  )

  lazy val kafkaDeps = Seq(
    "io.monix"          %% "monix-kafka-1x" % monixKafkaVersion,
    "org.apache.kafka"  % "kafka-clients"   % "1.0.0"     % "compile,test" exclude("org.slf4j", "slf4j-log4j12"),
    "com.typesafe.akka" %% "akka-testkit"   % akkaVersion % "test,it",
    scalaTest  % "test,it",
    logbackDep % "test,it")

  lazy val promDeps = Seq(
    "com.google.protobuf"    % "protobuf-java"             % "2.5.0",
    "org.scala-lang.modules" %% "scala-parser-combinators" % "2.3.0", // Updated for Scala 2.13
    "com.softwaremill.quicklens" %% "quicklens"            % "1.9.7", // Updated for Scala 2.13
    "org.antlr" % "antlr4-runtime" % "4.9.1"
  )

  lazy val gatewayDeps = commonDeps ++ Seq(
    logbackDep,
    "io.monix"   %% "monix-kafka-1x" % monixKafkaVersion,
    "org.rogach" %% "scallop"        % "3.3.0",  // Updated for Scala 2.13
    "com.google.protobuf" % "protobuf-java" % "2.5.0",
    "org.xerial.snappy" % "snappy-java" % "1.1.8.4",
    akkaHttp
  )

  lazy val httpDeps = Seq(
    logbackDep,
    akkaHttp,
    akkaHttpCirce,
    circeGeneric,
    circeParser,
    akkaHttpTestkit % Test,
    "org.xerial.snappy" % "snappy-java" % "1.1.8.4"
  )

  lazy val standaloneDeps = Seq(
    logbackDep,
    "com.iheart"            %% "ficus"                   % ficusVersion      % Test,
    "com.typesafe.akka"     %% "akka-multi-node-testkit" % akkaVersion       % Test,
    "com.softwaremill.sttp" %% "circe"                   % sttpVersion       % Test,
    "com.softwaremill.sttp" %% "akka-http-backend"       % sttpVersion       % Test,
    "com.softwaremill.sttp" %% "core"                    % sttpVersion       % Test,
    "com.typesafe.akka"     %% "akka-stream"             % akkaVersion       % Test // Updated to match akkaVersion
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
    "org.scalaj"                   %% "scalaj-http"             % "2.4.2",  // Updated for Scala 2.13
    "com.typesafe.akka"            %% "akka-testkit"            % akkaVersion   % Test,
    "com.typesafe.akka"            %% "akka-multi-node-testkit" % akkaVersion   % Test,
    scalaTest   % Test
  )

  //  lazy val sparkDeps = Seq(
  //    // We don't want LOG4J.  We want Logback!  The excludeZK is to help with a conflict re Coursier plugin.
  //    "org.apache.spark" %% "spark-hive"              % sparkVersion % "provided" excludeAll(excludeSlf4jLog4j, excludeZK),
  //    "org.apache.spark" %% "spark-hive-thriftserver" % sparkVersion % "provided" excludeAll(excludeSlf4jLog4j, excludeZK),
  //    "org.apache.spark" %% "spark-streaming"         % sparkVersion % "provided",
  //    scalaTest % "it"
  //  )

  lazy val jmhDeps = Seq(
    "org.apache.spark" %% "spark-sql" % sparkVersion excludeAll(excludeSlf4jLog4j, excludeZK, excludeJersey),
    "com.typesafe.akka"      %% "akka-testkit"                % akkaVersion
  )

  lazy val gatlingDeps = Seq(
      "io.gatling.highcharts" % "gatling-charts-highcharts" % "3.2.0" % "test,it",
      "io.gatling"            % "gatling-test-framework"    % "3.2.0" % "test,it"
  )

  //  lazy val stressDeps = Seq(
  //    "com.databricks"       %% "spark-csv"         % "1.3.0",
  //    scalaxyDep,
  //    "org.apache.spark"     %% "spark-sql"         % sparkVersion % "provided" excludeAll(excludeZK),
  //    "org.apache.spark"     %% "spark-streaming"   % sparkVersion % "provided" excludeAll(excludeZK)
  //  )
}
