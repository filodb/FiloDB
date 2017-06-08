
val akkaVersion = "2.3.16"
val kafkaVersion = "0.10.2.1"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaVersion % "compile,test",
  "com.typesafe.akka" %% "akka-remote" % akkaVersion % "compile,test",
  "org.typelevel" %% "cats-core" % "0.8.1",
  "org.apache.kafka" % "kafka-clients" % kafkaVersion % "compile,test",
  "com.cj" %% "kafka-rx" % "0.3.1" % "compile,test",
  "org.apache.kafka" % "kafka-streams" % kafkaVersion % "compile,test", //evaluating
  "org.scala-lang.modules" %% "scala-java8-compat" % "0.8.0",
  "io.monix" %% "monix-eval" % "2.1.2")

libraryDependencies ++= Seq(
  "joda-time" % "joda-time" % "2.9.7",
  "org.joda" % "joda-convert" % "1.8.1",
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
  "org.scalatest" %% "scalatest" % "2.2.4" % Test,// todo put back 3.0.1
  "ch.qos.logback" % "logback-classic" % "1.1.7" % Test
)
