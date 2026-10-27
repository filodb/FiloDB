// The FiloDB version. Change it on develop only. Merges carry it to integration and main.
ThisBuild / version := "1.0-SNAPSHOT"

// Publish pipelines set FILODB_RELEASE to make the integration or main version from the develop version:
//   FILODB_RELEASE=integration -> 1.0.integration-SNAPSHOT
//   FILODB_RELEASE=main        -> 1.0
// Local builds and develop builds keep the SNAPSHOT version.
ThisBuild / version ~= { v =>
  val base = v.stripSuffix("-SNAPSHOT")
  sys.env.get("FILODB_RELEASE").map(_.trim) match {
    case Some("integration") => s"$base.integration-SNAPSHOT"
    case Some("main")        => base
    case _                   => v
  }
}
