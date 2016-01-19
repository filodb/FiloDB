package filodb.core

import filodb.core._
import filodb.core.metadata.{Column, Dataset, RichProjection}
import filodb.core.store.{SegmentInfo, RowWriterSegment}
import org.velvia.filo.{RowReader, TupleRowReader}

object NamesTestData {
  val schema = Seq(Column("first", "dataset", 0, Column.ColumnType.StringColumn),
                   Column("last",  "dataset", 0, Column.ColumnType.StringColumn),
                   Column("age",   "dataset", 0, Column.ColumnType.LongColumn),
                   Column("seg",   "dataset", 0, Column.ColumnType.IntColumn))

  def mapper(rows: Seq[Product]): Iterator[RowReader] = rows.map(TupleRowReader).toIterator

  val dataset = Dataset("dataset", "age", "seg")
  val projection = RichProjection(dataset, schema)

  val names = Seq((Some("Khalil"),    Some("Mack"),     Some(24L), Some(0)),
                  (Some("Ndamukong"), Some("Suh"),      Some(28L), Some(0)),
                  (Some("Rodney"),    Some("Hudson"),   Some(25L), Some(0)),
                  (Some("Jerry"),     None,             Some(40L), Some(0)),
                  (Some("Peyton"),    Some("Manning"),  Some(39L), Some(0)),
                  (Some("Terrance"),  Some("Knighton"), Some(29L), Some(0)))

  def getRowWriter(segment: Int = 0): RowWriterSegment =
    new RowWriterSegment(projection, schema)(SegmentInfo("partition", segment).basedOn(projection))

  val firstNames = Seq("Khalil", "Rodney", "Ndamukong", "Terrance", "Peyton", "Jerry")

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

  val largeDataset = dataset.copy(options = Dataset.DefaultOptions.copy(chunkSize = 100),
                                  partitionColumns = Seq("league"))

  val lotLotNames = {
    for { league <- Seq("nfc", "afc")
          numChunks <- 0 to 2
          chunk  <- 0 to numChunks
          startRowNo = numChunks * 10000 + chunk * 100
          rowNo  <- startRowNo to (startRowNo + 99) }
    yield { (names(rowNo % 6)._1, names(rowNo % 6)._2,
             Some(rowNo.toLong),             // the unique row key
             Some(rowNo / 10000 * 10000),    // the segment key
             Some(league)) }                 // partition key
  }
}