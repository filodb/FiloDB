
libraryDependencies ++= Seq(
  //"com.cj" %% "kafka-rx" % "0.3.1" % "compile,test", //evaluating
  "org.typelevel" %% "cats-core" % "0.8.1",
  "org.apache.kafka" % "kafka-clients" % "0.10.2.1" % "compile,test" exclude("org.slf4j", "slf4j-log4j12"),
  "org.apache.kafka" % "kafka-streams" % "0.10.2.1" % "compile,test,it" exclude("org.slf4j", "slf4j-log4j12"), //evaluating
  "org.scala-lang.modules" %% "scala-java8-compat" % "0.8.0"
)

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-testkit" % "2.3.16" % "test,it",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test,it", // todo put back 3.0.1
  "ch.qos.logback" % "logback-classic" % "1.1.7" % "test,it"
)

scalacOptions ++= Seq(
  "-Xexperimental",
  "-deprecation",
  "-unchecked",
  "-Xfatal-warnings",
  "-Xlint",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Xfuture",
  "-Ywarn-dead-code",
  //"-Ywarn-value-discard",
  "-Ywarn-unused-import",
  "-encoding", "UTF-8",
  "-feature",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions"
) ++ (scalaBinaryVersion.value match {
  case v if v startsWith "2.12" => Nil
  case _ => Seq("-Yinline-warnings")
})

scalacOptions in(Compile, console) ~= (_.filterNot(_ == "-Ywarn-unused-import"))

scalacOptions in(Test, console) := (scalacOptions in(Compile, console)).value

javacOptions ++= Seq(
  "-Xlint",
  "-Xlint:unchecked",
  "-Xlint:deprecation",
  "-Xfatal-warnings",
  "-encoding", "UTF-8")

cancelable in Global := true

aggregate in update := false

updateOptions := updateOptions.value.withCachedResolution(true)

incOptions := incOptions.value.withNameHashing(true)

evictionWarningOptions in update := EvictionWarningOptions.default
  .withWarnTransitiveEvictions(false)
  .withWarnDirectEvictions(false)
  .withWarnScalaVersionEviction(false)

internalDependencyClasspath in IntegrationTest := (Classpaths.concat(
  internalDependencyClasspath in IntegrationTest, exportedProducts in Test)).value

fork in Test := true
fork in IntegrationTest := true

testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oDF")

addArtifact(artifact in (IntegrationTest, packageBin), packageBin in IntegrationTest)
publishArtifact in (IntegrationTest,packageBin) := true

def singleTests(tests: Seq[TestDefinition]): Seq[Tests.Group] =
  tests.map {
    case test if test.name contains "Converter" =>
      new Tests.Group(
        name = test.name,
        tests = Seq(test),
        runPolicy = Tests.SubProcess(ForkOptions(
          connectInput = true,
          runJVMOptions = Seq(
            "-Dfilodb.kafka.clients.config=./src/test/resources/kafka.client.properties"))))
    case test =>
      new Tests.Group(
        name = test.name,
        tests = Seq(test),
        runPolicy = Tests.SubProcess(ForkOptions(runJVMOptions = Seq.empty[String])))
  }

def itTests(tests: Seq[TestDefinition]): Seq[Tests.Group] =
  tests.map { case test =>
    new Tests.Group(
      name = test.name,
      tests = Seq(test),
      runPolicy = Tests.SubProcess(ForkOptions(
        connectInput = true,
        runJVMOptions = Seq(
          "-Dfilodb.kafka.clients.config=./kafka/src/test/resources/kafka.client.properties"))))
  }
testGrouping in Test := singleTests((definedTests in Test).value)
testGrouping in IntegrationTest := itTests((definedTests in IntegrationTest).value)
