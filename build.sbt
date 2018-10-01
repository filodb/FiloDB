// NOTE: Most of the build stuff is centralized in project/FiloBuild.scala.
// This is done to enable more flexible sharing of settings amongst multiple build.sbts for different build pipelines

publishTo      := Some(Resolver.file("Unused repo", file("target/unusedrepo")))
