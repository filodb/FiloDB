package filodb.core.reprojector

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{FileSystems, Files}

import com.typesafe.config.ConfigFactory
import filodb.core.{DatasetRef, GdeltTestData}
import filodb.core.metadata.{Column, DataColumn}
import org.scalatest.{BeforeAndAfter, FunSpec, Matchers}

/**
  * Created by parekuti on 8/10/16.
  */
class WriteAheadLogFileSpec extends FunSpec with Matchers with BeforeAndAfter{
  var config = ConfigFactory.load("application_test.conf")
    .getConfig("filodb")

  before {
    val tempDir = Files.createTempDirectory("wal")

    config = ConfigFactory.parseString(
      s"filodb.memtable.memtable-wal-dir = ${tempDir}")
      .withFallback(ConfigFactory.load("application_test.conf"))
      .getConfig("filodb")
  }
  it("creates memory mapped file with no data") {
    val wal = new WriteAheadLogFile(config, new DatasetRef("test"), 0)
    wal.file.exists() should equal(true)
    cleanUp(wal)
  }

  it("write header to the file") {
    val wal = new WriteAheadLogFile(config, new DatasetRef("test"), 0, GdeltTestData.createColumns(2))
    val expectedHeader = "\000LAWoliF\001\000\002\000\065\000]nmuloCgnirtS,0,1nmuloc,1[\001]nmuloCgnirtS,0,2nmuloc,2["
      .getBytes(StandardCharsets.UTF_8)
    wal.readHeader should equal(expectedHeader)
    cleanUp(wal)
  }

  private def cleanUp(wal: WriteAheadLogFile) = {
    wal.file.delete()
    wal.file.getParentFile.delete()
    wal.file.getParentFile.getParentFile.delete()
  }
}
