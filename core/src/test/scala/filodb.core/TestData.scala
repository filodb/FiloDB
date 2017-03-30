package filodb.core

import bloomfilter.mutable.BloomFilter
import java.nio.ByteBuffer
import java.sql.Timestamp
import org.joda.time.DateTime
import org.velvia.filo.{FiloVector, RowReader, TupleRowReader, ArrayStringRowReader, SeqRowReader}
import org.velvia.filo.ZeroCopyUTF8String._
import scala.io.Source
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

import filodb.core._
import filodb.core.binaryrecord.{BinaryRecord, RecordSchema}
import filodb.core.metadata.{Column, DataColumn, Dataset, RichProjection}
import filodb.core.query.{ChunkSetReader, PartitionChunkIndex, RowkeyPartitionChunkIndex}
import filodb.core.store._
import filodb.core.Types.PartitionKey

class TestSegmentState(projection: RichProjection,
                       index: PartitionChunkIndex,
                       schema: Seq[Column],
                       settings: SegmentStateSettings)
extends SegmentState(projection, index, schema, settings) {

  def this(projection: RichProjection,
           schema: Seq[Column],
           partition: PartitionKey = NamesTestData.defaultPartKey,
           settings: SegmentStateSettings = SegmentStateSettings()) =
    this(projection,
         new RowkeyPartitionChunkIndex(partition, projection),
         schema,
         settings)

  val makers = projection.rowKeyColumns.map(ChunkSetReader.defaultColumnToMaker).toArray

  val rowKeyChunks = new collection.mutable.HashMap[(BinaryRecord, Types.ChunkID), Array[ByteBuffer]]

  def getRowKeyVectors(key: BinaryRecord, chunkId: Types.ChunkID): Array[FiloVector[_]] =
    rowKeyChunks((key, chunkId)).zip(makers).map { case (buf, maker) => maker(buf, 5000) }

  def store(chunkSet: ChunkSet): Unit = {
    val rowKeyColNames = projection.rowKeyColumns.map(_.name)
    val chunkArray = rowKeyColNames.map(chunkSet.chunks).toArray
    rowKeyChunks((chunkSet.info.firstKey, chunkSet.info.id)) = chunkArray
  }

  def clear(): Unit = {
    rowKeyChunks.clear
  }
}

object NamesTestData {
  val schema = Seq(DataColumn(0, "first", "dataset", 0, Column.ColumnType.StringColumn),
                   DataColumn(1, "last",  "dataset", 0, Column.ColumnType.StringColumn),
                   DataColumn(2, "age",   "dataset", 0, Column.ColumnType.LongColumn),
                   DataColumn(3, "seg",   "dataset", 0, Column.ColumnType.IntColumn))

  def mapper(rows: Seq[Product]): Seq[RowReader] = rows.map(TupleRowReader)

  val dataset = Dataset("dataset", "age", "seg")
  val datasetRef = DatasetRef(dataset.name)
  val projection = RichProjection(dataset, schema)

  val names = Seq((Some("Khalil"),    Some("Mack"),     Some(24L), Some(0)),
                  (Some("Ndamukong"), Some("Suh"),      Some(28L), Some(0)),
                  (Some("Rodney"),    Some("Hudson"),   Some(25L), Some(0)),
                  (Some("Jerry"),     None,             Some(40L), Some(0)),
                  (Some("Peyton"),    Some("Manning"),  Some(39L), Some(0)),
                  (Some("Terrance"),  Some("Knighton"), Some(29L), Some(0)))

  val altNames = Seq((Some("Stacy"),    Some("McGee"),     Some(24L), Some(0)),
                  (Some("Bruce"),     Some("Irvin"),    Some(28L), Some(0)),
                  (Some("Amari"),     Some("Cooper"),   Some(25L), Some(0)),
                  (Some("Jerry"),     None,             Some(40L), Some(0)),
                  (Some("Derek"),     Some("Carr"),     Some(39L), Some(0)),
                  (Some("Karl"),      Some("Joseph"),   Some(29L), Some(0)))

  val firstKey = BinaryRecord(projection, Seq(names.head._3.get))
  val lastKey = BinaryRecord(projection, Seq(names.last._3.get))
  def keyForName(rowNo: Int): BinaryRecord = BinaryRecord(projection, Seq(names(rowNo)._3.getOrElse(0)))

  val defaultPartKey = BinaryRecord(projection.partKeyBinSchema, SeqRowReader(Seq("/0")))

  val stateSettings = SegmentStateSettings()
  val emptyFilter = SegmentState.emptyFilter(stateSettings)

  def getState(segment: Int = 0): TestSegmentState = new TestSegmentState(projection, schema)

  def getWriterSegment(segment: Int = 0): ChunkSetSegment =
    new ChunkSetSegment(projection, SegmentInfo(defaultPartKey, segment).basedOn(projection))

  val firstNames = names.map(_._1.get)
  val utf8FirstNames = firstNames.map(_.utf8)
  val sortedFirstNames = Seq("Khalil", "Rodney", "Ndamukong", "Terrance", "Peyton", "Jerry")
  val sortedUtf8Firsts = sortedFirstNames.map(_.utf8)

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

  val badLine = ArrayStringRowReader(Array("NotANumber"))   // Will fail
  val altLines =
    """0,1979-01-01,197901,1979,AFR,africa,5,5.52631578947368
      |1,1979-01-01,197901,1979,AGR,farm-yo,6,10.9792284866469
      |2,1979-01-01,197901,1979,AGR,farm-yo,6,10.9792284866469""".stripMargin.split("\n")
  val altReaders = altLines.map { line => ArrayStringRowReader(line.split(",")) }

  val schema = Seq(DataColumn(0, "GLOBALEVENTID", "gdelt", 0, Column.ColumnType.IntColumn),
                   DataColumn(1, "SQLDATE",       "gdelt", 0, Column.ColumnType.TimestampColumn),
                   DataColumn(2, "MonthYear",     "gdelt", 0, Column.ColumnType.IntColumn),
                   DataColumn(3, "Year",          "gdelt", 0, Column.ColumnType.IntColumn),
                   DataColumn(4, "Actor2Code",    "gdelt", 0, Column.ColumnType.StringColumn),
                   DataColumn(5, "Actor2Name",    "gdelt", 0, Column.ColumnType.StringColumn),
                   DataColumn(6, "NumArticles",   "gdelt", 0, Column.ColumnType.IntColumn),
                   DataColumn(7, "AvgTone",       "gdelt", 0, Column.ColumnType.DoubleColumn))
  val binSchema = RecordSchema(schema)

  case class GdeltRecord(eventId: Int, sqlDate: Long, monthYear: Int, year: Int,
                         actor2Code: String, actor2Name: String, numArticles: Int, avgTone: Double)

  val records = gdeltLines.map { line =>
    val parts = line.split(',')
    GdeltRecord(parts(0).toInt, DateTime.parse(parts(1)).getMillis,
                parts(2).toInt, parts(3).toInt,
                parts(4), parts(5), parts(6).toInt, parts(7).toDouble)
  }
  val seqReaders = records.map { record => SeqRowReader(record.productIterator.toList) }

  // Dataset1: Partition keys (Actor2Code, Year) / Row key GLOBALEVENTID / Seg :string 0
  val dataset1 = Dataset("gdelt", Seq("GLOBALEVENTID"), ":string 0", Seq("Actor2Code", "Year"))
  val projection1 = RichProjection(dataset1, schema)

  // Dataset2: Partition key (MonthYear) / Row keys (Actor2Code, GLOBALEVENTID)
  // Segment ID is to group GLOBALEVENTID such that there will be two segments
  val dataset2 = Dataset("gdelt", Seq("Actor2Code", "GLOBALEVENTID"),
                         ":round GLOBALEVENTID 50", Seq("MonthYear"))
  val projection2 = RichProjection(dataset2, schema)

  // Dataset3: same as Dataset1 but with :getOrElse to prevent null partition keys
  val dataset3 = Dataset("gdelt", Seq("GLOBALEVENTID"), ":string 0",
                         Seq(":getOrElse Actor2Code NONE", ":getOrElse Year -1"))
  val projection3 = RichProjection(dataset3, schema)

  // Dataset4: One big partition (Year) and segment (:string 0) with (Actor2Code, GLOBALEVENTID) rowkey
  // to easily test row key scans
  val dataset4 = Dataset("gdelt", Seq("Actor2Code", "GLOBALEVENTID"), ":string 0", Seq("Year"))
  val projection4 = RichProjection(dataset4, schema)

  // Dataset 5: partition :monthYear SQLDATE, rowkey (Actor2Code, GLOBALEVENTID)
  // to test timestamp processing
  val dataset5 = Dataset("gdelt", Seq("Actor2Code", "GLOBALEVENTID"), ":string 0",
                                  Seq(":monthOfYear SQLDATE"))
  val projection5 = RichProjection(dataset5, schema)

  def getSegments(partKey: PartitionKey): Seq[(ChunkSetSegment, Seq[RowReader])] = {
    val segInfo = SegmentInfo(partKey, "").basedOn(projection2)
    val seg = new ChunkSetSegment(projection2, segInfo)
    Seq((seg, readers.toBuffer))
  }

  // Returns projection1 or 2 segments grouped by partition
  def getSegmentsByPartKey(projection: RichProjection): Seq[(ChunkSetSegment, Seq[RowReader])] = {
    val inputGroupedBySeg = readers.toSeq.groupBy(projection.partitionKeyFunc)
    inputGroupedBySeg.map { case (partKey, lines) =>
      val segInfo = SegmentInfo(partKey, "").basedOn(projection)
      val seg = new ChunkSetSegment(projection, segInfo)
      (seg, lines)
    }.toSeq
  }

  def createColumns(count: Int) : Seq[Column] = {
    if (count == 0){
      Nil
    } else{
      val fieldName = s"column$count"
      new DataColumn(count,fieldName,"testtable",0,Column.ColumnType.StringColumn) +: createColumns(count - 1)
    }
  }
}