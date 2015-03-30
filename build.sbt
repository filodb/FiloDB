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
  "com.beachape"         %% "enumeratum"        % "0.0.4",
  "org.velvia.filo"      %% "filo-scala"        % "0.0.4",
  "com.websudos"         %% "phantom-testing"   % phantomVersion % "test",
  "com.typesafe.akka"    %% "akka-testkit"      % akkaVersion % "test"
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
