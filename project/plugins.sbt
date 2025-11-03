addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.1.5")  // Updated for SBT 1.9.x

addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.4.7")

addSbtPlugin("com.typesafe.sbt" % "sbt-multi-jvm" % "0.4.0")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.10.0-RC1")

addSbtPlugin("io.kamon" % "sbt-kanela-runner" % "2.0.6")

addSbtPlugin("io.gatling" % "gatling-sbt" % "3.2.2")

addSbtPlugin("com.github.sbt" % "sbt-protobuf" % "0.7.2")

resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.0.11")  // Updated to fix scala-xml conflict

addSbtPlugin("com.sonar-scala" % "sbt-sonar" % "2.3.0")

// Force scala-xml version to resolve conflict
dependencyOverrides += "org.scala-lang.modules" %% "scala-xml" % "2.1.0"
