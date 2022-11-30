addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.5")

addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.3.7")

addSbtPlugin("com.typesafe.sbt" % "sbt-multi-jvm" % "0.4.0")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.10.0-RC1")

addSbtPlugin("io.kamon" % "sbt-kanela-runner" % "2.0.6")

addSbtPlugin("io.gatling" % "gatling-sbt" % "3.2.2")

resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"
