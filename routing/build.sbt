
val akkaVersion = "2.3.16"
val kafkaVersion = "0.10.2.1"

// Many of the following deps may be removed in the next week
libraryDependencies ++= Seq(
  "com.typesafe.akka"      %% "akka-actor"         % akkaVersion  % "compile,test",
  "com.typesafe.akka"      %% "akka-remote"        % akkaVersion  % "compile,test",
  "com.typesafe.akka"      %% "akka-stream-kafka"  % "0.16",
  "org.typelevel"          %% "cats-core"          % "0.8.1",
  "org.apache.kafka"       % "kafka-clients"       % kafkaVersion % "compile,test",
  "org.apache.kafka"       % "kafka-streams"       % kafkaVersion,
  "io.monix"               %% "monix-eval"         % "2.1.2",
  // TODO remove hackathon:
  "com.google.protobuf"    % "protobuf-java" % "3.0.0"
)

libraryDependencies ++= Seq(
  "org.scala-lang.modules" %% "scala-java8-compat" % "0.8.0",
  "joda-time"              % "joda-time"           % "2.9.7",
  "org.joda"               % "joda-convert"        % "1.8.1",
  "com.typesafe.akka"      %% "akka-testkit"       % akkaVersion  % Test,
  "org.scalatest"          % "scalatest_2.11"      % "3.0.1"      % Test,
  "ch.qos.logback"         % "logback-classic"     % "1.1.7"      % Test
  //"com.typesafe.akka" %% "akka-cluster-sharding" % akkaVersion  % Test,
  //"com.typesafe.akka" %% "akka-distributed-data" % akkaVersion  % Test,
  //"com.typesafe.akka" %% "akka-stream"           % akkaVersion  % Test,
  //"com.typesafe.akka" %% "akka-stream-testkit"   % akkaVersion  % Test,
)
