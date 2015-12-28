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
                 .settings(cliAssemblySettings:_*)
                 .dependsOn(core, coordinator, cassandra,spark)

lazy val spark = (project in file("spark"))
                   .settings(mySettings:_*)
                   .settings(name := "filodb-spark")
                   .settings(libraryDependencies ++= sparkDeps)
                   .settings(assemblySettings:_*)
                   .settings(assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = true))
                   .dependsOn(core % "compile->compile; test->test")
                   .dependsOn(cassandra % "compile->compile; test->test")

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
  "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2",
  "com.clearspring.analytics" % "stream"        % "2.7.0",
  "it.unimi.dsi"          % "dsiutils"          % "2.2.4",
  "ch.qos.logback"        % "logback-classic"   % "1.0.7",
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
    exclude("com.esotericsoftware.minlog", "minlog")
)

lazy val sparkDeps = Seq(
  "org.jboss.aesh"        % "aesh"              % "0.66",
  "com.github.lalyos"     % "jfiglet"           % "0.0.7",
  "org.jboss.aesh"        % "aesh-extensions"   % "0.66",
  "org.apache.spark"     %% "spark-sql"         % "1.4.1"  % "provided"
)

//////////////////////////
///

lazy val coreSettings = Seq(
  scalacOptions ++= Seq("-Xlint", "-deprecation", "-Xfatal-warnings", "-feature","-no-specialization")
)

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

lazy val shellScript = """#!/usr/bin/env sh
exec java -Xmx4g -Xms4g -jar "$0" "$@"
""".split("\n")

val scalaVer = "2.10"

lazy val cliAssemblySettings =  Seq(
  assemblyJarName in assembly := s"filo-cli-${version.value}",
  logLevel in assembly := Level.Error,
  assemblyMergeStrategy in assembly := {
    case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
    case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
    case m if m.toLowerCase.matches("meta-inf.*\\.properties") => MergeStrategy.discard
    case PathList(ps @ _*) if ps.last endsWith ".txt.1" => MergeStrategy.first
    case "reference.conf" => MergeStrategy.concat
    case "application.conf"                            => MergeStrategy.concat
    case m if m.toLowerCase.endsWith("org.apache.hadoop.fs.filesystem") => MergeStrategy.concat
    case x => MergeStrategy.last
  },
  assemblyExcludedJars in assembly := { val cp = (fullClasspath in assembly).value
      val includesJar = Set(
        "javax.inject.jar", "aopalliance.jar", "unused-1.0.0.jar",
        "RoaringBitmap-0.4.5.jar", "zookeeper-3.4.5.jar", "cglib-2.2.1-v20090111.jar",
        "akka-remote_2.10-2.3.4-spark.jar", "asm-3.2.jar", "spark-sql_2.10-1.4.1.jar",
        "hadoop-common-2.2.0.jar","jaxb-api-2.2.2.jar",
        "jersey-test-framework-grizzly2-1.9.jar","jersey-server-1.9.jar",
        "jline-0.9.94.jar","activation-1.1.jar","jackson-jaxrs-1.8.8.jar",
        "javax.servlet-3.0.0.v201112011016.jar","jackson-xc-1.8.8.jar",
        "jersey-guice-1.9.jar","jersey-json-1.9.jar","commons-lang3-3.3.2.jar",
        "jul-to-slf4j-1.7.10.jar","akka-actor_2.10-2.3.4-spark.jar",
        "hadoop-yarn-client-2.2.0.jar","jcl-over-slf4j-1.7.10.jar",
        "jettison-1.1.jar","hadoop-annotations-2.2.0.jar","stax-api-1.0.1.jar",
        "slf4j-log4j12-1.7.10.jar","hadoop-mapreduce-client-core-2.2.0.jar",
        "compress-lzf-1.0.3.jar","jaxb-impl-2.2.3-1.jar","commons-cli-1.2.jar",
        "commons-math-2.1.jar","snappy-java-1.1.1.7.jar","netty-all-4.0.23.Final.jar",
        "lz4-1.2.0.jar","jackson-annotations-2.4.4.jar","xmlenc-0.52.jar",
        "commons-httpclient-3.1.jar","spark-core_2.10-1.4.1.jar",
        "hadoop-yarn-server-common-2.2.0.jar","commons-net-3.1.jar","netty.jar",
        "ivy-2.4.0.jar","spark-network-shuffle_2.10-1.4.1.jar","spark-unsafe_2.10-1.4.1.jar",
        "hadoop-mapreduce-client-shuffle-2.2.0.jar","hadoop-mapreduce-client-jobclient-2.2.0.jar",
        "scalap-2.10.0.jar","log4j-1.2.17.jar","jets3t-0.7.1.jar","oro-2.0.8.jar",
        "protobuf-java-2.5.0-spark.jar","jackson-databind-2.4.4.jar","curator-recipes-2.4.0.jar",
        "tachyon-client-0.6.4.jar","uncommons-maths.jar","avro-1.7.4.jar","curator-framework-2.4.0.jar",
        "akka-slf4j_2.10-2.3.4-spark.jar","json4s-jackson_2.10-3.2.10.jar","commons-compress-1.4.1.jar",
        "jackson-core-2.4.4.jar","curator-client-2.4.0.jar","jersey-core-1.9.jar","tachyon-0.6.4.jar",
        "json4s-core_2.10-3.2.10.jar","xz-1.0.jar","parquet-column-1.6.0rc3.jar","protobuf-java-2.5.0.jar",
        "json4s-ast_2.10-3.2.10.jar","mesos-0.21.1-shaded-protobuf.jar","metrics-core-3.1.0.jar",
        "metrics-jvm-3.1.0.jar","hadoop-auth-2.2.0.jar","commons-codec-1.5.jar","parquet-common-1.6.0rc3.jar",
        "parquet-encoding-1.6.0rc3.jar","pyrolite-4.4.jar","parquet-generator-1.6.0rc3.jar",
        "parquet-hadoop-1.6.0rc3.jar","hadoop-hdfs-2.2.0.jar","metrics-json-3.1.0.jar","py4j-0.8.2.1.jar",
        "parquet-format-2.2.0-rc1.jar","metrics-graphite-3.1.0.jar","jackson-module-scala_2.10-2.4.4.jar",
        "parquet-jackson-1.6.0rc3.jar","spark-catalyst_2.10-1.4.1.jar","jackson-mapper-asl-1.9.11.jar",
        "jackson-core-asl-1.9.11.jar","jetty-util-6.1.26.jar","jodd-core-3.6.3.jar",
        "hadoop-mapreduce-client-app-2.2.0.jar","chill_2.10-0.5.0.jar","quasiquotes_2.10-2.0.1.jar",
        "chill-java-0.5.0.jar","kryo-2.21.jar","reflectasm-1.07-shaded.jar","minlog-1.2.jar",
        "objenesis-1.2.jar","hadoop-client-2.2.0.jar",
        "hadoop-mapreduce-client-common-2.2.0.jar","hadoop-yarn-common-2.2.0.jar","hadoop-yarn-api-2.2.0.jar","guice-3" +
          ".0.jar",
        s"spark-core_$scalaVer-1.4.1.jar", s"spark-catalyst_$scalaVer-1.4.1.jar",
        s"spark-network-common_$scalaVer-1.4.1.jar", s"spark-network-sql_$scalaVer-1.4.1.jar",
        s"spark-launcher_$scalaVer-1.4.1.jar"
      )
      cp filterNot { jar => includesJar.contains(jar.data.getName)}
    },
  test in assembly := {} //noisy for end-user since the jar is not available and user needs to build the project locally
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

zip <<=
  (Keys.target, Keys.name, Keys
    .version) map { (target: File, name: String, version: String) =>
    val distdir = target / (name + "-" + version)
    val zipFile = target / (name + "-" + version + ".zip")

    IO.delete(zipFile)
    IO.delete(distdir)

    val lib = distdir / "lib"
    val bin = distdir / "bin"
    IO.createDirectories(Seq(distdir, lib, bin))
    val base = file(".")

    IO.copyFile(base / s"cli/target/scala-$scalaVer/filo-cli-$version", lib /
      s"filodb-cli-$version.jar", true)
    IO.copyFile(base / s"spark/target/scala-$scalaVer/filodb-spark-assembly-$version.jar", lib /
      s"filodb-spark-$version.jar", true)
    IO.copyFile(base / "bin/filo-sh", bin / "filosh", true)

    def entries(f: File): List[File] = f :: (if (f.isDirectory) IO.listFiles(f).toList.flatMap(entries(_)) else Nil)
    IO.zip(entries(distdir).map(d => (d, d.getAbsolutePath.substring(distdir.getParent.length))), zipFile)
    zipFile
  }

addCommandAlias("dist", ";clean;spark/assembly;cli/assembly;zip")