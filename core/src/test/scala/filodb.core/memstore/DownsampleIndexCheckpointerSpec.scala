package filodb.core.memstore

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.io.File

class DownsampleIndexCheckpointerSpec extends AnyFunSpec {

  it("should return 0 on an empty folder") {
    val baseDir = new File(System.getProperty("java.io.tmpdir"))
    val baseName = s"DownsampleIndexCheckpointerSpec-${System.currentTimeMillis()}-"
    val tempDir = new File(baseDir, baseName)
    tempDir.mkdir()
    //val c = new DownsampleIndexCheckpointer();
    val time = DownsampleIndexCheckpointer.getDownsampleLastCheckpointTime(tempDir)
    time shouldEqual 0
    DownsampleIndexCheckpointer.writeCheckpoint(tempDir, 10)
    val time2 = DownsampleIndexCheckpointer.getDownsampleLastCheckpointTime(tempDir)
    time2 shouldEqual 10
  }
}