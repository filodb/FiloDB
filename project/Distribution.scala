import sbt._
import Keys._

object Distribution {

  val scalaVer = "2.10"

  val zipTask =
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

  val jars = Set(
    "javax.inject.jar", "aopalliance.jar", "unused-1.0.0.jar",
    "RoaringBitmap-0.4.5.jar", "zookeeper-3.4.5.jar", "cglib-2.2.1-v20090111.jar",
    "akka-remote_2.10-2.3.4-spark.jar", "asm-3.2.jar", "spark-sql_2.10-1.4.1.jar",
    "hadoop-common-2.2.0.jar", "jaxb-api-2.2.2.jar",
    "jersey-test-framework-grizzly2-1.9.jar", "jersey-server-1.9.jar",
    "jline-0.9.94.jar", "activation-1.1.jar", "jackson-jaxrs-1.8.8.jar",
    "javax.servlet-3.0.0.v201112011016.jar", "jackson-xc-1.8.8.jar",
    "jersey-guice-1.9.jar", "jersey-json-1.9.jar", "commons-lang3-3.3.2.jar",
    "jul-to-slf4j-1.7.10.jar", "akka-actor_2.10-2.3.4-spark.jar",
    "hadoop-yarn-client-2.2.0.jar", "jcl-over-slf4j-1.7.10.jar",
    "jettison-1.1.jar", "hadoop-annotations-2.2.0.jar", "stax-api-1.0.1.jar",
    "slf4j-log4j12-1.7.10.jar", "hadoop-mapreduce-client-core-2.2.0.jar",
    "compress-lzf-1.0.3.jar", "jaxb-impl-2.2.3-1.jar", "commons-cli-1.2.jar",
    "commons-math-2.1.jar", "snappy-java-1.1.1.7.jar", "netty-all-4.0.23.Final.jar",
    "lz4-1.2.0.jar", "jackson-annotations-2.4.4.jar", "xmlenc-0.52.jar",
    "commons-httpclient-3.1.jar", "spark-core_2.10-1.4.1.jar",
    "hadoop-yarn-server-common-2.2.0.jar", "commons-net-3.1.jar", "netty.jar",
    "ivy-2.4.0.jar", "spark-network-shuffle_2.10-1.4.1.jar", "spark-unsafe_2.10-1.4.1.jar",
    "hadoop-mapreduce-client-shuffle-2.2.0.jar", "hadoop-mapreduce-client-jobclient-2.2.0.jar",
    "scalap-2.10.0.jar", "log4j-1.2.17.jar", "jets3t-0.7.1.jar", "oro-2.0.8.jar",
    "protobuf-java-2.5.0-spark.jar", "jackson-databind-2.4.4.jar", "curator-recipes-2.4.0.jar",
    "tachyon-client-0.6.4.jar", "uncommons-maths.jar", "avro-1.7.4.jar", "curator-framework-2.4.0.jar",
    "akka-slf4j_2.10-2.3.4-spark.jar", "json4s-jackson_2.10-3.2.10.jar", "commons-compress-1.4.1.jar",
    "jackson-core-2.4.4.jar", "curator-client-2.4.0.jar", "jersey-core-1.9.jar", "tachyon-0.6.4.jar",
    "json4s-core_2.10-3.2.10.jar", "xz-1.0.jar", "parquet-column-1.6.0rc3.jar", "protobuf-java-2.5.0.jar",
    "json4s-ast_2.10-3.2.10.jar", "mesos-0.21.1-shaded-protobuf.jar", "metrics-core-3.1.0.jar",
    "metrics-jvm-3.1.0.jar", "hadoop-auth-2.2.0.jar", "commons-codec-1.5.jar", "parquet-common-1.6.0rc3.jar",
    "parquet-encoding-1.6.0rc3.jar", "pyrolite-4.4.jar", "parquet-generator-1.6.0rc3.jar",
    "parquet-hadoop-1.6.0rc3.jar", "hadoop-hdfs-2.2.0.jar", "metrics-json-3.1.0.jar", "py4j-0.8.2.1.jar",
    "parquet-format-2.2.0-rc1.jar", "metrics-graphite-3.1.0.jar", "jackson-module-scala_2.10-2.4.4.jar",
    "parquet-jackson-1.6.0rc3.jar", "spark-catalyst_2.10-1.4.1.jar", "jackson-mapper-asl-1.9.11.jar",
    "jackson-core-asl-1.9.11.jar", "jetty-util-6.1.26.jar", "jodd-core-3.6.3.jar",
    "hadoop-mapreduce-client-app-2.2.0.jar", "chill_2.10-0.5.0.jar", "quasiquotes_2.10-2.0.1.jar",
    "chill-java-0.5.0.jar", "kryo-2.21.jar", "reflectasm-1.07-shaded.jar", "minlog-1.2.jar",
    "objenesis-1.2.jar", "hadoop-client-2.2.0.jar",
    "hadoop-mapreduce-client-common-2.2.0.jar", "hadoop-yarn-common-2.2.0.jar", "hadoop-yarn-api-2.2.0.jar", "guice-3" +
      ".0.jar",
    s"spark-core_$scalaVer-1.4.1.jar", s"spark-catalyst_$scalaVer-1.4.1.jar",
    s"spark-network-common_$scalaVer-1.4.1.jar", s"spark-network-sql_$scalaVer-1.4.1.jar",
    s"spark-launcher_$scalaVer-1.4.1.jar"
  )
}