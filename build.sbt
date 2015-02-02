val mySettings = Seq(organization := "org.velvia",
                     scalaVersion := "2.10.4",
                     resolvers ++= extraRepos)

lazy val core = (project in file("core"))
                  .settings(mySettings:_*)
                  .settings(name := "filodb-core")
                  .settings(libraryDependencies ++= coreDeps)

val phantomVersion = "1.5.0"
val akkaVersion    = "2.3.7"

lazy val extraRepos = Seq(
  "Typesafe repository releases" at "http://repo.typesafe.com/typesafe/releases/",
  "twitter-repo" at "http://maven.twttr.com",
  "websudos-repo" at "http://maven.websudos.co.uk/ext-release-local"
)

lazy val coreDeps = Seq(
  "com.websudos"         %% "phantom-dsl"       % phantomVersion,
  "com.websudos"         %% "phantom-zookeeper" % phantomVersion,
  "com.typesafe.akka"    %% "akka-slf4j"        % akkaVersion,
  "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2",
  "ch.qos.logback"        % "logback-classic"   % "1.0.7",
  "com.websudos"         %% "phantom-testing"   % phantomVersion % "test",
  "com.typesafe.akka"    %% "akka-testkit"      % akkaVersion % "test"
)
