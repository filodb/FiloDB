package filodb.core

import filodb.core._
import filodb.core.metadata.{Column, DataColumn, Dataset, RichProjection}
import filodb.core.store.{SegmentInfo, RowWriterSegment}
import java.sql.Timestamp
import org.joda.time.DateTime
import org.velvia.filo.{RowReader, TupleRowReader, ArrayStringRowReader}
import scala.io.Source

object NamesTestData {
  val schema = Seq(DataColumn(0, "first", "dataset", 0, Column.ColumnType.StringColumn),
                   DataColumn(1, "last",  "dataset", 0, Column.ColumnType.StringColumn),
                   DataColumn(2, "age",   "dataset", 0, Column.ColumnType.LongColumn),
                   DataColumn(3, "seg",   "dataset", 0, Column.ColumnType.IntColumn))

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
    DataColumn(4, "league", "dataset", 0, Column.ColumnType.StringColumn)
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

/**
 * The first 99 rows of the GDELT data set, from a few select columns, enough to really play around
 * with different layouts and multiple partition as well as row keys.  And hey it's real data!
 */
object GdeltTestData {
  val gdeltLines = Source.fromURL(getClass.getResource("/GDELT-sample-test.csv"))
                         .getLines.toSeq.drop(1)     // drop the header line

  val readers = gdeltLines.map { line => ArrayStringRowReader(line.split(",")) }

  val schema = Seq(DataColumn(0, "GLOBALEVENTID", "gdelt", 0, Column.ColumnType.IntColumn),
                   DataColumn(1, "SQLDATE",       "gdelt", 0, Column.ColumnType.TimestampColumn),
                   DataColumn(2, "MonthYear",     "gdelt", 0, Column.ColumnType.IntColumn),
                   DataColumn(3, "Year",          "gdelt", 0, Column.ColumnType.IntColumn),
                   DataColumn(4, "Actor2Code",    "gdelt", 0, Column.ColumnType.StringColumn),
                   DataColumn(5, "Actor2Name",    "gdelt", 0, Column.ColumnType.StringColumn),
                   DataColumn(6, "NumArticles",   "gdelt", 0, Column.ColumnType.IntColumn),
                   DataColumn(7, "AvgTone",       "gdelt", 0, Column.ColumnType.DoubleColumn))

  case class GdeltRecord(eventId: Int, sqlDate: Timestamp, monthYear: Int, year: Int,
                         actor2Code: String, actor2Name: String, numArticles: Int, avgTone: Double)

  val records = gdeltLines.map { line =>
    val parts = line.split(',')
    GdeltRecord(parts(0).toInt, new Timestamp(DateTime.parse(parts(1)).getMillis),
                parts(2).toInt, parts(3).toInt,
                parts(4), parts(5), parts(6).toInt, parts(7).toDouble)
  }

  // Dataset1: Partition keys (Actor2Code, Year) / Row key GLOBALEVENTID / Seg :string 0
  val dataset1 = Dataset("gdelt", Seq("GLOBALEVENTID"), ":string 0", Seq("Actor2Code", "Year"))
  val projection1 = RichProjection(dataset1, schema)

  // Dataset2: Partition key (MonthYear) / Row keys (Actor2Code, GLOBALEVENTID)
  // Segment ID is to group GLOBALEVENTID such that there will be two segments
  val dataset2 = Dataset("gdelt", Seq(":getOrElse Actor2Code NONE", "GLOBALEVENTID"),
                         ":round GLOBALEVENTID 50", Seq("MonthYear"))
  val projection2 = RichProjection(dataset2, schema)

  // Dataset3: same as Dataset1 but with :getOrElse to prevent null partition keys
  val dataset3 = Dataset("gdelt", Seq("GLOBALEVENTID"), ":string 0",
                         Seq(":getOrElse Actor2Code NONE", ":getOrElse Year -1"))
  val projection3 = RichProjection(dataset3, schema)

  // Returns projection2 grouped by segment with a fake partition key
  def getSegments(partKey: projection2.PK): Seq[RowWriterSegment] = {
    val inputGroupedBySeg = readers.toSeq.groupBy(projection2.segmentKeyFunc)
                                   .toSeq.sortBy(_._1.asInstanceOf[Int])
    inputGroupedBySeg.map { case (segmentKey, lines) =>
      val seg = new RowWriterSegment(projection2, schema)(SegmentInfo(partKey, segmentKey).
                                                          basedOn(projection2))
      seg.addRowsAsChunk(lines.toIterator)
      seg
    }
  }

  // Returns projection1 or 2 segments grouped by partition and segment key
  def getSegmentsByPartKey(projection: RichProjection): Seq[RowWriterSegment] = {
    val inputGroupedBySeg = readers.toSeq.groupBy(r => (projection.partitionKeyFunc(r),
                                                        projection.segmentKeyFunc(r)))
    inputGroupedBySeg.map { case ((partKey, segKey), lines) =>
      val seg = new RowWriterSegment(projection, schema)(SegmentInfo(partKey, segKey).
                                                          basedOn(projection))
      seg.addRowsAsChunk(lines.toIterator)
      seg
    }.toSeq
  }
}