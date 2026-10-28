package filodb.core.memstore

import java.nio.file.Files
import java.util.concurrent.atomic.AtomicLong

import org.apache.lucene.store.{Directory, IOContext, MMapDirectory, NIOFSDirectory}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class ByteCountingDirectorySpec extends AnyFunSpec with Matchers {

  private val FileName = "test.bin"
  private val FileLength = 100

  // Writes a 100 byte file ("0" .. "99" as bytes) to the directory.
  private def writeFile(dir: Directory): Unit = {
    val out = dir.createOutput(FileName, IOContext.DEFAULT)
    (0 until FileLength).foreach(i => out.writeByte(i.toByte))
    out.close()
  }

  private def bytesRead[T](body: => T): Long = {
    val before = ByteCountingDirectory.threadBytesRead()
    body
    ByteCountingDirectory.threadBytesRead() - before
  }

  private def directories(): Seq[(String, Directory)] = {
    val mmapPath = Files.createTempDirectory("bcd-mmap")
    val nioPath = Files.createTempDirectory("bcd-nio")
    Seq("MMapDirectory" -> new MMapDirectory(mmapPath), "NIOFSDirectory" -> new NIOFSDirectory(nioPath))
  }

  directories().foreach { case (name, rawDir) =>
    describe(s"ByteCountingDirectory over $name") {
      val dir = new ByteCountingDirectory(rawDir)
      writeFile(dir)

      it("should count readByte, readBytes, readInt and readLong") {
        val in = dir.openInput(FileName, IOContext.DEFAULT)
        bytesRead(in.readByte()) shouldEqual 1
        bytesRead(in.readBytes(new Array[Byte](10), 0, 10)) shouldEqual 10
        bytesRead(in.readInt()) shouldEqual 4
        bytesRead(in.readLong()) shouldEqual 8
        in.close()
      }

      it("should count variable length ints once per byte read") {
        val in = dir.openInput(FileName, IOContext.DEFAULT)
        // Bytes 0..127 all have the high bit clear, so a vInt is exactly 1 byte here.
        bytesRead(in.readVInt()) shouldEqual 1
        in.close()
      }

      it("should not count seek, getFilePointer or length") {
        val in = dir.openInput(FileName, IOContext.DEFAULT)
        bytesRead { in.seek(50); in.getFilePointer; in.length() } shouldEqual 0
        in.close()
      }

      it("should count reads on clones and slices on the same thread") {
        val in = dir.openInput(FileName, IOContext.DEFAULT)
        val clone = in.clone()
        bytesRead(clone.readByte()) shouldEqual 1
        val slice = in.slice("slice", 10, 50)
        bytesRead(slice.readBytes(new Array[Byte](5), 0, 5)) shouldEqual 5
        in.close()
      }

      it("should count reads through randomAccessSlice") {
        val in = dir.openInput(FileName, IOContext.DEFAULT)
        val ra = in.randomAccessSlice(0, FileLength.toLong)
        bytesRead(ra.readLong(0)) shouldEqual 8
        in.close()
      }

      it("should keep the counter separate for each thread") {
        val in = dir.openInput(FileName, IOContext.DEFAULT)
        val otherThreadDelta = new AtomicLong(-1)
        val before = ByteCountingDirectory.threadBytesRead()
        val t = new Thread(() => {
          val c = in.clone()
          val b = ByteCountingDirectory.threadBytesRead()
          c.readBytes(new Array[Byte](20), 0, 20)
          otherThreadDelta.set(ByteCountingDirectory.threadBytesRead() - b)
        })
        t.start()
        t.join()
        otherThreadDelta.get() shouldEqual 20
        ByteCountingDirectory.threadBytesRead() shouldEqual before
        in.close()
      }

      it("should credit reads to the thread that created the clone") {
        val in = dir.openInput(FileName, IOContext.DEFAULT)
        val clone = in.clone() // created on this thread
        val before = ByteCountingDirectory.threadBytesRead()
        val otherThreadDelta = new AtomicLong(-1)
        val t = new Thread(() => {
          val b = ByteCountingDirectory.threadBytesRead()
          clone.readBytes(new Array[Byte](20), 0, 20) // read on another thread
          otherThreadDelta.set(ByteCountingDirectory.threadBytesRead() - b)
        })
        t.start()
        t.join()
        // The bytes are credited to the creating thread (this one), not to the reading thread.
        otherThreadDelta.get() shouldEqual 0
        ByteCountingDirectory.threadBytesRead() - before shouldEqual 20
        in.close()
      }
    }
  }
}
