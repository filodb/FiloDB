addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.9")

addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.4.7")

addSbtPlugin("com.typesafe.sbt" % "sbt-multi-jvm" % "0.4.0")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.10.0-RC1")

addSbtPlugin("io.kamon" % "sbt-kanela-runner" % "2.0.6")

addSbtPlugin("io.gatling" % "gatling-sbt" % "3.2.2")

addSbtPlugin("com.github.sbt" % "sbt-protobuf" % "0.7.2")

resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.0.7")
