package filodb.core

import scala.io.Source

import monix.reactive.Observable
import org.joda.time.DateTime

import filodb.core.binaryrecord.BinaryRecord
import filodb.core.memstore.{IngestRecord, IngestRouting}
import filodb.core.metadata.{Column, Dataset}
import filodb.core.store._
import filodb.core.Types.PartitionKey
import filodb.memory.format._
import filodb.memory.format.ZeroCopyUTF8String._

object TestData {
  def toChunkSetStream(ds: Dataset,
                       part: PartitionKey,
                       rows: Seq[RowReader],
                       rowsPerChunk: Int = 10): Observable[ChunkSet] =
    Observable.fromIterator(rows.grouped(rowsPerChunk).map { chunkRows => ChunkSet(ds, part, chunkRows) })
}

object NamesTestData {
  def mapper(rows: Seq[Product]): Seq[RowReader] = rows.map(TupleRowReader)

  val dataColSpecs = Seq("first:string", "last:string", "age:long")
  val dataset = Dataset("dataset", Seq("seg:int"), dataColSpecs, "age")

  // NOTE: first 3 columns are the data columns, thus names could be used for either complete record
  // or the data column rowReader
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

  val firstKey = dataset.rowKey(mapper(names).head)
  val lastKey = dataset.rowKey(mapper(names).last)
  def keyForName(rowNo: Int): BinaryRecord = dataset.rowKey(mapper(names)(rowNo))

  val defaultPartKey = dataset.partKey(0)

  def chunkSetStream(data: Seq[Product] = names): Observable[ChunkSet] =
    TestData.toChunkSetStream(dataset, defaultPartKey, mapper(data))

  val firstNames = names.map(_._1.get)
  val utf8FirstNames = firstNames.map(_.utf8)
  val sortedFirstNames = Seq("Khalil", "Rodney", "Ndamukong", "Terrance", "Peyton", "Jerry")
  val sortedUtf8Firsts = sortedFirstNames.map(_.utf8)

  val largeDataset = Dataset("dataset", Seq("league:string"), dataColSpecs, "age")

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

  val schema = Seq("GLOBALEVENTID:int",
                   "SQLDATE:ts",
                   "MonthYear:int",
                   "Year:int",
                   "Actor2Code:string",
                   "Actor2Name:string",
                   "NumArticles:int",
                   "AvgTone:double")
  val columnNames = schema.map(_.split(':').head)

  // WARNING: do not use these directly with toChunkSetStream, you won't get the right fields.
  // Please go through records() / IngestRecords for proper field routing
  val readers = gdeltLines.map { line => ArrayStringRowReader(line.split(",")) }

  def records(ds: Dataset, readerSeq: Seq[RowReader] = readers): Seq[IngestRecord] = {
    val routing = IngestRouting(ds, columnNames)
    readerSeq.zipWithIndex.map { case (reader, idx) => IngestRecord(routing, reader, idx) }
  }

  def dataRows(ds: Dataset, readerSeq: Seq[RowReader] = readers): Seq[RowReader] =
    records(ds, readerSeq).map(_.data)

  val badLine = ArrayStringRowReader("NotANumber, , , , , , ,".split(','))   // Will fail
  val altLines =
    """0,1979-01-01,197901,1979,AFR,africa,5,5.52631578947368
      |1,1979-01-01,197901,1979,AGR,farm-yo,6,10.9792284866469
      |2,1979-01-01,197901,1979,AGR,farm-yo,6,10.9792284866469""".stripMargin.split("\n")
  val altReaders = altLines.map { line => ArrayStringRowReader(line.split(",")) }

  case class GdeltRecord(eventId: Int, sqlDate: Long, monthYear: Int, year: Int,
                         actor2Code: String, actor2Name: String, numArticles: Int, avgTone: Double)

  val records = gdeltLines.map { line =>
    val parts = line.split(',')
    GdeltRecord(parts(0).toInt, DateTime.parse(parts(1)).getMillis,
                parts(2).toInt, parts(3).toInt,
                parts(4), parts(5), parts(6).toInt, parts(7).toDouble)
  }
  val seqReaders = records.map { record => SeqRowReader(record.productIterator.toList) }

  // Dataset1: Partition keys (Actor2Code, Year) / Row key GLOBALEVENTID
  val dataset1 = Dataset("gdelt", Seq(schema(4), schema(3)), schema.patch(3, Nil, 2), "GLOBALEVENTID")

  // Dataset2: Partition key (MonthYear) / Row keys (Actor2Code, GLOBALEVENTID)
  val dataset2 = Dataset("gdelt", Seq(schema(2)), schema.patch(2, Nil, 1), Seq("Actor2Code", "GLOBALEVENTID"))

  // Dataset3: same as Dataset1 for now
  val dataset3 = dataset1

  // Dataset4: One big partition (Year) with (Actor2Code, GLOBALEVENTID) rowkey
  // to easily test row key scans
  val dataset4 = Dataset("gdelt", Seq(schema(3)), schema.patch(3, Nil, 1), Seq("Actor2Code", "GLOBALEVENTID"))

  // Proj 6: partition Actor2Code,Actor2Name to test partition key bitmap indexing
  val dataset6 = Dataset("gdelt", schema.slice(4, 6), schema.patch(4, Nil, 2), "GLOBALEVENTID")

  // Returns projection1 or 2 segments grouped by partition
  def getStreamsByPartKey(dataset: Dataset): Seq[Observable[ChunkSet]] = {
    val inputGroupedBySeg = records(dataset, readers).toSeq.groupBy(_.partition)
    inputGroupedBySeg.map { case (partReader, records) =>
      val partKey = dataset.partKey(partReader)
      TestData.toChunkSetStream(dataset, partKey, records.map(_.data))
    }.toSeq
  }

  def getRowsByPartKey(dataset: Dataset): Seq[(PartitionKey, Seq[RowReader])] =
    records(dataset, readers).toSeq.groupBy(_.partition).toSeq.map { case (partReader, records) =>
      (dataset.partKey(partReader), records.map(_.data))
    }
}

// A simulation of machine metrics data
object MachineMetricsData {
  import scala.util.Random.nextInt

  import Column.ColumnType._

  val columns = Seq("timestamp:long", "min:double", "avg:double", "max:double", "p90:double")

  def singleSeriesData(initTs: Long = System.currentTimeMillis,
                       incr: Long = 1000): Stream[Product] = {
    Stream.from(0).map { n =>
      (Some(initTs + n * incr),
       Some((45 + nextInt(10)).toDouble),
       Some((60 + nextInt(25)).toDouble),
       Some((100 + nextInt(15)).toDouble),
       Some((85 + nextInt(12)).toDouble))
    }
  }

  def singleSeriesReaders(): Stream[RowReader] = singleSeriesData().map(TupleRowReader)

  // Dataset1: Partition keys (series) / Row key timestamp
  val dataset1 = Dataset("metrics", Seq("series:string"), columns)
  val defaultPartKey = dataset1.partKey("series0")

  // Turns either multiSeriesData() or linearMultiSeries() into IngestRecord's
  def records(stream: Stream[Seq[Any]]): Stream[IngestRecord] =
    stream.zipWithIndex.map { case (values, index) =>
      IngestRecord(SchemaSeqRowReader(values drop 5, Array(StringColumn.keyType.extractor)),
                   SeqRowReader(values take 5),
                   index)
    }

  def multiSeriesData(): Stream[Seq[Any]] = {
    val initTs = System.currentTimeMillis
    Stream.from(0).map { n =>
      Seq(initTs + n * 1000,
         (45 + nextInt(10)).toDouble,
         (60 + nextInt(25)).toDouble,
         (100 + nextInt(15)).toDouble,
         (85 + nextInt(12)).toDouble,
         "Series " + (n % 10))
    }
  }

  // Everything increments by 1 for simple predictability and testing
  def linearMultiSeries(startTs: Long = 100000L, numSeries: Int = 10): Stream[Seq[Any]] = {
    Stream.from(0).map { n =>
      Seq(startTs + n * 1000,
         (1 + n).toDouble,
         (20 + n).toDouble,
         (100 + n).toDouble,
         (85 + n).toDouble,
         "Series " + (n % numSeries))
    }
  }

  val dataset2 = Dataset("metrics", Seq("series:string", "tags:map"), columns)

  def withMap(data: Stream[Seq[Any]], n: Int = 5): Stream[Seq[Any]] =
    data.zipWithIndex.map { case (row, idx) => row :+ Map("n".utf8 -> (idx % n).toString.utf8) }
}
