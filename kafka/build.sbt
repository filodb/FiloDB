
libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "0.10.2.1" % "compile,test" exclude("org.slf4j", "slf4j-log4j12"),
  "io.monix" %% "monix-kafka-10" % "0.14",
  // test dependencies:
  "com.typesafe.akka" %% "akka-testkit" % "2.3.16" % "test,it",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test,it", // todo put back 3.0.1
  "ch.qos.logback" % "logback-classic" % "1.1.7" % "test,it"
)

scalacOptions ++= Seq(
  "-deprecation",
  "-unchecked",
  "-encoding", "UTF-8",
  "-feature",
  "-Xfuture",
  "-Xexperimental",
  "-Xfatal-warnings",
  "-Ywarn-inaccessible",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
 //todo "-Ywarn-numeric-widen",
  "-Ywarn-dead-code",
  "-Ywarn-unused-import",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-language:experimental.macros"
)

scalacOptions ++= (CrossVersion.partialVersion(scalaVersion.value) match {
  case Some((2, majorVersion)) if majorVersion >= 11 =>
    Seq(
      "-Yinline-warnings",
      // "-Xlint",
      // linter options
      "-Xlint:adapted-args",
      "-Xlint:nullary-unit",
      "-Xlint:inaccessible",
      "-Xlint:nullary-override",
      "-Xlint:infer-any",
      "-Xlint:missing-interpolator",
      "-Xlint:doc-detached",
      "-Xlint:private-shadow",
      "-Xlint:type-parameter-shadow",
      "-Xlint:poly-implicit-overload",
      "-Xlint:option-implicit",
      "-Xlint:delayedinit-select",
      "-Xlint:by-name-right-associative",
      "-Xlint:package-object-classes",
      "-Xlint:unsound-match"
    )
  case _ =>
    Nil
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

def singleTests(tests: Seq[TestDefinition], path: String): Seq[Tests.Group] =
  tests.map {
    case test if (test.name contains "Converter") || (test.name contains "Config") =>
      new Tests.Group(
        name = test.name,
        tests = Seq(test),
        runPolicy = Tests.SubProcess(ForkOptions(
          connectInput = true,
          runJVMOptions = Seq(
            s"-Dfilodb.kafka.config.file=$path/src/test/resources/full-test.properties"))))
    case test if test.name contains "Settings" =>
      new Tests.Group(
        name = test.name,
        tests = Seq(test),
        runPolicy = Tests.SubProcess(ForkOptions(
          connectInput = true,
          runJVMOptions = Seq(
            s"-Dfilodb.kafka.config.file=$path/src/test/resources/kafka.client.properties"))))
    case test =>
      new Tests.Group(
        name = test.name,
        tests = Seq(test),
        runPolicy = Tests.SubProcess(ForkOptions(runJVMOptions = Seq.empty[String])))
  }

def itTests(tests: Seq[TestDefinition], path: String): Seq[Tests.Group] =
  tests.map { test =>
    new Tests.Group(
      name = test.name,
      tests = Seq(test),
      runPolicy = Tests.SubProcess(ForkOptions(
        connectInput = true,
        runJVMOptions = Seq(
          s"-Dfilodb.kafka.config.file=$path/src/test/resources/full-test.properties"))))
  }
testGrouping in Test := singleTests((definedTests in Test).value, baseDirectory.value.getAbsolutePath)
testGrouping in IntegrationTest := itTests((definedTests in IntegrationTest).value, baseDirectory.value.getAbsolutePath)
