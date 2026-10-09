import scala.sys.process._
import scala.util.Try

/**
 * Makes the build version from a base version and the current git branch.
 *
 * The base version lives in version.sbt. Change it on develop only.
 * Merges from develop to integration to main carry it to the other branches.
 *
 *   develop     -> <base>-SNAPSHOT              (for example 1.0-SNAPSHOT)
 *   integration -> <base>.integration-SNAPSHOT  (for example 1.0.integration-SNAPSHOT)
 *   main        -> <base>                       (for example 1.0)
 *   other       -> <base>-SNAPSHOT
 *
 * To set the branch by hand (for example in a CI job with a detached HEAD), set FILODB_BRANCH.
 */
object FiloVersion {

  // Checked in order. The first one that is set and not empty gives the branch.
  //   FILODB_BRANCH   - manual override
  //   GITHUB_HEAD_REF - GitHub Actions, pull request source branch
  //   GITHUB_REF_NAME - GitHub Actions, push branch
  //   BRANCH_NAME     - Jenkins multibranch
  //   GIT_BRANCH      - Jenkins git plugin (for example origin/develop)
  private val BranchEnvVars = Seq("FILODB_BRANCH", "GITHUB_HEAD_REF", "GITHUB_REF_NAME", "BRANCH_NAME", "GIT_BRANCH")

  def fromBranch(base: String): String = forBranch(base, currentBranch)

  def forBranch(base: String, branch: String): String = branch match {
    case "main"        => base
    case "integration" => s"$base.integration-SNAPSHOT"
    case _             => s"$base-SNAPSHOT"
  }

  def currentBranch: String = {
    val fromEnv = BranchEnvVars.iterator.flatMap(sys.env.get).map(_.trim).find(_.nonEmpty)
    val fromGit = Try(Process(Seq("git", "rev-parse", "--abbrev-ref", "HEAD")).!!(ProcessLogger(_ => ())).trim)
      .toOption.filter(b => b.nonEmpty && b != "HEAD")
    fromEnv.orElse(fromGit).map(normalize).getOrElse("")
  }

  private def normalize(branch: String): String =
    branch.stripPrefix("refs/heads/").stripPrefix("origin/")
}
