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

lazy val cli = (project in file("cli"))
                 .settings(mySettings:_*)
                 .settings(name := "filodb-cli")
                 .settings(libraryDependencies ++= cliDeps)
                 .dependsOn(core)

val phantomVersion = "1.5.0"
val akkaVersion    = "2.3.7"

lazy val extraRepos = Seq(
  "Typesafe repository releases" at "http://repo.typesafe.com/typesafe/releases/",
  "twitter-repo" at "http://maven.twttr.com",
  "websudos-repo" at "http://maven.websudos.co.uk/ext-release-local",
  "Velvia Bintray" at "https://dl.bintray.com/velvia/maven"
)

lazy val coreDeps = Seq(
  "com.websudos"         %% "phantom-dsl"       % phantomVersion,
  "com.websudos"         %% "phantom-zookeeper" % phantomVersion,
  "com.typesafe.akka"    %% "akka-slf4j"        % akkaVersion,
  "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2",
  "ch.qos.logback"        % "logback-classic"   % "1.0.7",
  "com.beachape"         %% "enumeratum"        % "1.2.1",
  "org.velvia.filo"      %% "filo-scala"        % "0.1.0",
  "com.opencsv"           % "opencsv"           % "3.3",
  "com.nativelibs4java"  %% "scalaxy-loops"     % "0.3.3" % "provided",
  "com.websudos"         %% "phantom-testing"   % phantomVersion % "test",
  "com.typesafe.akka"    %% "akka-testkit"      % akkaVersion % "test"
)

lazy val cliDeps = Seq(
  "com.quantifind"       %% "sumac"             % "0.3.0"
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
