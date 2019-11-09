package filodb.memory.format

import java.sql.Timestamp

import org.scalatest.{FunSpec, Matchers}

import filodb.memory.NativeMemoryManager
import filodb.memory.format.MemoryReader._
import filodb.memory.format.vectors.{IntBinaryVector, LongBinaryVector}

class RowReaderTest extends FunSpec with Matchers {
  val memFactory = new NativeMemoryManager(100000)
  val acc = nativePtrReader
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

  def readValues[T](r: MutableFiloRowReader, len: Int)(f: RowReader => T): Seq[T] = {
    (0 until len).map { i =>
      r.rowNo = i
      f(r)
    }
  }

  it("should read longs from timestamp strings from ArrayStringRowReader") {
    ArrayStringRowReader(csvRows.head).getLong(2) should equal(96768000000L)
  }


  it("should append to BinaryAppendableVector from Readers using addFromReader") {
    val readers = rows.map(TupleRowReader)
    val appenders = Seq(
      IntBinaryVector.appendingVector(memFactory, 10),
      LongBinaryVector.appendingVector(memFactory, 10)
    )
    readers.foreach { r => appenders.zipWithIndex.foreach { case (a, i) => a.addFromReader(r, i + 1) } }
    val ptrs = appenders.map(_.optimize(memFactory)).toArray
    val reader = new
        MutableFiloRowReader {
      def reader(columnNo: Int): VectorDataReader = columnNo match {
        case 0 => IntBinaryVector(acc, ptrs(0))
        case 1 => LongBinaryVector(acc, ptrs(1))
      }
      def vectAddr(columnNo: Int): BinaryVector.BinaryVectorPtr = ptrs(columnNo)
      def vectAccessor(columnNo: Int): MemoryReader = acc
    }

    readValues(reader, 4)(_.getInt(0)) should equal(Seq(18, 0, 59, 26))
    reader.rowNo = 1
    // reader.notNull(0) should equal(false)    // notNull does not work since isAvailable is not available everywhere
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