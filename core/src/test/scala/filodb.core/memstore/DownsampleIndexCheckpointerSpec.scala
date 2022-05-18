package filodb.core.memstore

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.io.{File, FileWriter}

class DownsampleIndexCheckpointerSpec extends AnyFunSpec {

  it("should return 0 on an empty folder") {
    val baseDir = new File(System.getProperty("java.io.tmpdir"))
    val baseName = s"DownsampleIndexCheckpointerSpec-empty-folder-${System.currentTimeMillis()}-"
    val tempDir = new File(baseDir, baseName)
    tempDir.mkdir()
    val time = DownsampleIndexCheckpointer.getDownsampleLastCheckpointTime(tempDir)
    time shouldEqual 0
  }

  it("should return 0 on non-existent folder") {
    val baseDir = new File(System.getProperty("java.io.tmpdir"))
    val baseName = s"DownsampleIndexCheckpointerSpec-non-existent-folder-${System.currentTimeMillis()}-"
    val tempDir = new File(baseDir, baseName)
    val time2 = DownsampleIndexCheckpointer.getDownsampleLastCheckpointTime(tempDir)
    time2 shouldEqual 0
  }

  it("should write and be able to read it back") {
    val baseDir = new File(System.getProperty("java.io.tmpdir"))
    val baseName = s"DownsampleIndexCheckpointerSpec-write-read-${System.currentTimeMillis()}-"
    val tempDir = new File(baseDir, baseName)
    tempDir.mkdir()
    DownsampleIndexCheckpointer.writeCheckpoint(tempDir, 10)
    val time2 = DownsampleIndexCheckpointer.getDownsampleLastCheckpointTime(tempDir)
    time2 shouldEqual 10
  }

  it("should return 0 on damaged checkpoint file") {
    val baseDir = new File(System.getProperty("java.io.tmpdir"))
    val baseName = s"DownsampleIndexCheckpointerSpec-damaged-${System.currentTimeMillis()}-"
    val tempDir = new File(baseDir, baseName)
    tempDir.mkdir()
    val file : File = new File(tempDir, DownsampleIndexCheckpointer.CHECKPOINT_FILE_NAME)
    val fw : FileWriter = new FileWriter(file);
    fw.write("garbage")
    fw.close()
    val time = DownsampleIndexCheckpointer.getDownsampleLastCheckpointTime(tempDir)
    time shouldEqual 0
  }

}