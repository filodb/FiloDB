package filodb.core

import java.nio.ByteBuffer

import filodb.core.metadata.{KeyRange, Column}
import filodb.core.store.{Dataset, MetaStore}
import org.velvia.filo.{FiloRowReader, FastFiloRowReader, RowReader, TupleRowReader}

object Setup{
  type RowReaderFactory = (Array[ByteBuffer], Array[Class[_]]) => FiloRowReader
  val readerFactory: RowReaderFactory = (bytes, classes) => new FastFiloRowReader(bytes, classes)

  val schema = Seq(
    Column("country", "dataset", 0, Column.ColumnType.StringColumn),
    Column("city", "dataset", 0, Column.ColumnType.StringColumn),
    Column("first", "dataset", 0, Column.ColumnType.StringColumn),
    Column("last", "dataset", 0, Column.ColumnType.StringColumn),
    Column("age", "dataset", 0, Column.ColumnType.LongColumn))

  def mapper(rows: Seq[Product]): Iterator[RowReader] = rows.map(TupleRowReader).toIterator

  // primary key and segment are same
  val dataset = Dataset("dataset",schema,"country" ,"first","age","city")
  val projection =dataset.projections.seq(0)
  val keyRange = KeyRange("A", "Z")
  val DefaultPartitionKey = "Single"

  val names = Seq(
    (Some("US"),Some("SF"),Some("Khalil"   ),   Some("Mack"    ),  Some(24L)),
    (Some("US"),Some("NY"),Some("Ndamukong"),   Some("Suh"     ),  Some(28L)),
    (Some("US"),Some("NY"),Some("Rodney"   ),   Some("Hudson"  ),  Some(25L)),
    (Some("UK"),Some("LN"),Some("Jerry"    ),   None,              Some(40L)),
    (Some("UK"),Some("LN"),Some("Peyton"   ),   Some("Manning" ),  Some(39L)),
    (Some("UK"),Some("LN"),Some("Terrance" ),   Some("Knighton"),  Some(29L)))



  val firstNames = Seq("Khalil", "Ndamukong", "Rodney", "Terrance", "Peyton", "Jerry")


  // OK, what we want is to test multiple partitions, segments, multiple chunks per segment too.
  // With default segmentSize of 10000, change chunkSize to say 100.
  // Thus let's have the following:
  // "nfc"  0-99  10000-10099 10100-10199  20000-20099 20100-20199 20200-20299
  // "afc"  the same
  // 1200 rows total, 6 segments (3 x 2 partitions)
  // No need to test out of order since that's covered by other things (but we can scramble the rows
  // just for fun)
  val schemaWithPartCol = schema ++ Seq(
    Column("league", "dataset", 0, Column.ColumnType.StringColumn)
  )

  val lotLotNames = {
    for { league <- Seq("nfc", "afc")
          numChunks <- 0 to 2
          chunk  <- 0 to numChunks
          startRowNo = numChunks * 10000 + chunk * 100
          rowNo  <- startRowNo to (startRowNo + 99) }
      yield { (names(rowNo % 6)._1, names(rowNo % 6)._2, Some(rowNo.toLong), Some(league)) }
  }
}
