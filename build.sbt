val mySettings = Seq(organization := "org.velvia",
                     scalaVersion := "2.10.4")

lazy val core = (project in file("core"))
                  .settings(mySettings:_*)
                  .settings(name := "filodb-core")
                  .settings(libraryDependencies ++= coreDeps)

val phantomVersion = "1.5.0"

lazy val coreDeps = Seq(
  "com.websudos"         %% "phantom-dsl"       % phantomVersion,
  "com.websudos"         %% "phantom-zookeeper" % phantomVersion,
  "org.scalatest"        %% "scalatest"         % "2.1.0" % "test"
)

resolvers ++= Seq(
  "Typesafe repository releases" at "http://repo.typesafe.com/typesafe/releases/",
  "twitter-repo" at "http://maven.twttr.com",
  "websudos-repo" at "http://maven.websudos.co.uk/ext-release-local"
)
