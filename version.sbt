// Base version. Change it on develop only. Merges carry it to integration and main.
// The git branch adds the suffix (see project/FiloVersion.scala):
//   develop -> 1.0-SNAPSHOT, integration -> 1.0.integration-SNAPSHOT, main -> 1.0
ThisBuild / version := FiloVersion.fromBranch("1.0")
