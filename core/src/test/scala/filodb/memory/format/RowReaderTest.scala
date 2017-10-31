package filodb.memory.format

import java.sql.Timestamp

import filodb.memory.NativeMemoryManager
import filodb.memory.format.vectors.{IntBinaryVector, LongBinaryVector}

import org.scalatest.{FunSpec, Matchers}

class RowReaderTest extends FunSpec with Matchers {
  val memFactory = new NativeMemoryManager(100000)
  val rows = Seq(
    (Some("Matthew Perry"), Some(18), Some(new Timestamp(10000L))),
    (Some("Michelle Pfeiffer"), None, Some(new Timestamp(10010L))),
    (Some("George C"), Some(59), None),
    (Some("Rich Sherman"), Some(26), Some(new Timestamp(10000L)))
  )

  val csvRows = Seq(
    "Matthew Perry,18,1973-01-25T00Z",
    "Michelle Pfeiffer,,1970-07-08T00Z",
    "George C,59,",
    "Rich Sherman,26,1991-10-12T00Z"
  ).map(str => (str.split(',') :+ "").take(3))

  def readValues[T](r: FastFiloRowReader, len: Int)(f: FiloRowReader => T): Seq[T] = {
    (0 until len).map { i =>
      r.rowNo = i
      f(r)
    }
  }

  it("should read longs from timestamp strings from ArrayStringRowReader") {
    ArrayStringRowReader(csvRows.head).getLong(2) should equal(96768000000L)
  }


  it("should append to BinaryAppendableVector from Readers with RowReaderAppender") {
    val readers = rows.map(TupleRowReader)
    val appenders = Seq(
      new IntReaderAppender(IntBinaryVector.appendingVector(memFactory, 10), 1),
      new LongReaderAppender(LongBinaryVector.appendingVector(memFactory, 10), 2)
    )
    readers.foreach { r => appenders.foreach(_.append(r)) }
    val bufs = appenders.map(_.appender.optimize(memFactory).toFiloBuffer).toArray
    val reader = new FastFiloRowReader(bufs, Array(classOf[Int], classOf[Long]))

    readValues(reader, 4)(_.getInt(0)) should equal(Seq(18, 0, 59, 26))
    reader.rowNo = 1
    reader.notNull(0) should equal(false)
  }

  import filodb.memory.format.RowReader._

  it("should compare RowReaders using TypedFieldExtractor") {
    val readers = rows.map(TupleRowReader)
    StringFieldExtractor.compare(readers(1), readers(2), 0) should be > (0)
    IntFieldExtractor.compare(readers(0), readers(2), 1) should be < (0)
    TimestampFieldExtractor.compare(readers(0), readers(3), 2) should equal(0)

    // Ok, we should be able to compare the reader with the NA / None too
    IntFieldExtractor.compare(readers(1), readers(2), 1) should be < (0)
  }
}