// Used by RustPlugin to look at current OS info
libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.14.0"

// sbt-scoverage 2.2.x needs scala-xml 2.3.0 but sbt-sonar pulls scalariform which needs 1.0.6;
// using Always scheme lets sbt evict to the higher version without treating it as a binary conflict
libraryDependencySchemes += "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always