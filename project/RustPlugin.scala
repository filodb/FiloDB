import org.apache.commons.lang3._
import sbt._
import sbt.Keys._
import sbt.io.Path._
import sbt.nio.Keys._
import scala.sys.process._

/*
  Plugin that adds support to build native Rust code as part of a module.
  This will build the code, include it in resources, and allow runtime loading.
 */
object RustPlugin extends AutoPlugin {
  object autoImport {
    // Tasks
    val rustCompile = taskKey[Unit]("Compile rust code for this module.")
    val rustClean = taskKey[Unit]("Clean rust build for this module.")
    val rustGatherLibraries = taskKey[Seq[(File, String)]]("Gather the list of native libraries produced by the build.")
    val rustLint = taskKey[Unit]("Run linting on rust code for this module.")
    val rustTest = taskKey[Unit]("Test rust code for this module.")

    // Settings
    val rustSourceDir = settingKey[File]("Path to base directory with rust code.")
    val rustArchitectures = settingKey[Seq[String]]("List of architectures to build for.  Takes either a Rust " +
      "target tuple or the special key 'host' to build for the current machine.  To supply multiple architectures " +
      "separate them with a ';' character.")
    val rustFeatures = settingKey[String]("Value to pass to cargo's --features option.  Defaults to an empty string.")
    var rustOptimize = settingKey[Boolean]("Enable optimization during rust builds.  Defaults to false for host " +
      "only builds and true for any other configuration.")
  }

  import autoImport._

  lazy val settings: Seq[Setting[_]] = Seq(
    rustSourceDir := baseDirectory.value / "src" / "rust",
    rustArchitectures := {
      val archs = Option(System.getProperty("rust.architectures")).getOrElse("host")

      archs.split(';').toSeq
    },
    rustFeatures := {
      val features = Option(System.getProperty("rust.features")).getOrElse("")

      features
    },
    rustOptimize := {
      val optimize = Option(System.getProperty("rust.optimize")).getOrElse(
        if (rustArchitectures.value.length > 1) {
          "true"
        } else {
          "false"
        })

      optimize.toBoolean
    },
    rustClean := {
      val log = streams.value.log
      val sourceDir = rustSourceDir.value

      log.info(s"Cleaning rust source at $sourceDir")

      val returnCode = Process(s"cargo clean", sourceDir) ! cargoLog(log)

      if (returnCode != 0)
        sys.error(s"cargo clean failed with exit code $returnCode")
    },
    rustCompile := {
      val log = streams.value.log
      val sourceDir = rustSourceDir.value
      val features = rustFeatures.value

      for (archTarget <- rustArchitectures.value) {
        log.info(s"Compiling rust source at $sourceDir for architecture $archTarget with features '$features'")

        // target setup
        val targetCommand = if (archTarget == "host") {
          ""
        } else {
          s"--target $archTarget"
        }

        // Use build for the host target, zigbuild for everything else
        val buildCommand  = if (archTarget == "host") {
          "build"
        } else {
          "zigbuild"
        }

        // Check if release build
        val buildFlags = if (rustOptimize.value) {
          "--release"
        } else {
          ""
        }

        val featureFlags = if (features.isBlank) {
          ""
        } else {
          s" --features $features "
        }

        val returnCode = Process(s"cargo $buildCommand $buildFlags $featureFlags $targetCommand",
          sourceDir) ! cargoLog(log)

        if (returnCode != 0)
          sys.error(s"cargo build failed with exit code $returnCode")
      }
    },
    rustGatherLibraries := {
      val log = streams.value.log
      var list: Seq[(File, String)] = Seq()

      // Compile first
      rustCompile.value

      val release = rustOptimize.value
      val releaseDir = if (release) {
        "release"
      } else {
        "debug"
      }

      val targetFolder = rustSourceDir.value / "target"
      val fileTree = fileTreeView.value

      // For each architecture find artifacts
      for (archTarget <- rustArchitectures.value) {
        // Special case - host
        val archFolder = if (archTarget == "host") {
          targetFolder / releaseDir
        } else {
          // General case
          targetFolder / archTarget / releaseDir
        }

        // get os arch / kernel, build path
        val resourceArchTarget = mapRustTargetToJVMTarget(archTarget)

        // Find library files in folder
        // We place every produced library in a resource path like
        // /native/<kernel>/<arch>/<file>
        val glob = fileTree.list(Glob(archFolder) / "*.{so,dylib}").collect {
          case (path, attributes) if attributes.isRegularFile => file(path.toString)
        }
        val files = glob.pair(rebase(archFolder, s"/native/$resourceArchTarget"))

        list = list ++ files
      }

      list
    },
    rustTest := {
      val log = streams.value.log
      val sourceDir = rustSourceDir.value

      val returnCode = Process(s"cargo test", sourceDir) ! cargoLog(log)

      returnCode match {
        case 101 => sys.error("One or more tests failed")
        case 0 => ()
        case x => sys.error(s"cargo test failed with exit code $x")
      }
    },
    resourceGenerators += Def.task {
      val log = streams.value.log

      val libraries: Seq[(File, String)] = rustGatherLibraries.value
      val resources: Seq[File] = for ((file, path) <- libraries) yield {
        val resource = resourceManaged.value / path

        if (IO.getModifiedTimeOrZero(file) > IO.getModifiedTimeOrZero(resource)) {
          IO.copyFile(file, resource, preserveLastModified = true)
        }
        resource
      }
      resources
    }.taskValue
  )

  lazy val testSettings: Seq[Setting[_]] = Seq(
    rustLint := {
      val log = streams.value.log
      val sourceDir = rustSourceDir.value

      val returnCode = Process(s"cargo clippy --all-targets -- -D warnings", sourceDir) ! cargoLog(log)
      if (returnCode != 0)
        sys.error(s"cargo clippy failed with exit code $returnCode")
    },
    test := {
      // Run rust tests and linting
      rustLint.value
      rustTest.value
      // Run base test task
      test.value
    }
  )

  // Map an input Rust arch tuple to the correct target folder for JVM loading
  private def mapRustTargetToJVMTarget(arch: String): String = {
    // Rust tuples are basically clang tuples and look like:
    // aarch64-apple-darwin
    // x86_64-unknown-linux-gnu
    //
    // We want the architecture (first part)
    // and the kernel part (third part)
    val RustPattern = "([^-]+)-([^-]+)-([^-]+).*".r
    arch match {
      case "host" => s"$getHostKernel/${SystemUtils.OS_ARCH}"
      case RustPattern(arch, _, kernel) => s"$kernel/$arch"
      case x => sys.error(s"Unsupported architecture $x")
    }
  }

  // Get normalized host kernel name
  private def getHostKernel: String = {
    if (SystemUtils.IS_OS_LINUX) {
      "linux"
    } else if (SystemUtils.IS_OS_MAC) {
      "darwin"
    } else if (SystemUtils.IS_OS_WINDOWS) {
      "windows"
    } else {
      sys.error(s"Unhandled platform ${SystemUtils.OS_NAME}")
    }
  }

  // Cargo logs to both stdout and stderr with normal output
  // Log all of these events as info
  private def cargoLog(log: sbt.Logger): ProcessLogger = new ProcessLogger {
    def out(s: => String): Unit = log.info(s)
    def err(s: => String): Unit = log.info(s)
    def buffer[T](f: => T): T = f
  }

  override lazy val projectSettings = inConfig(Compile)(settings) ++ inConfig(Test)(settings ++ testSettings)
}
