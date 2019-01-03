package filodb.core

import scala.io.Source

import com.typesafe.config.ConfigFactory
import monix.eval.Task
import monix.reactive.Observable
import org.joda.time.DateTime

import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.memstore.SomeData
import filodb.core.metadata.Column.ColumnType
import filodb.core.metadata.{Dataset, DatasetOptions}
import filodb.core.store._
import filodb.core.Types.{PartitionKey, UTF8Map}
import filodb.memory.format._
import filodb.memory.format.ZeroCopyUTF8String._
import filodb.memory.{BinaryRegionLarge, MemFactory, NativeMemoryManager}

object TestData {
  def toChunkSetStream(ds: Dataset,
                       part: PartitionKey,
                       rows: Seq[RowReader],
                       rowsPerChunk: Int = 10): Observable[ChunkSet] =
    Observable.fromIterator(rows.grouped(rowsPerChunk).map { chunkRows => ChunkSet(ds, part, chunkRows, nativeMem) })

  def toRawPartData(chunkSetStream: Observable[ChunkSet]): Task[RawPartData] = {
    var partKeyBytes: Array[Byte] = null
    chunkSetStream.map { case ChunkSet(info, partKey, _, chunks, _) =>
                    if (partKeyBytes == null) {
                      partKeyBytes = BinaryRegionLarge.asNewByteArray(UnsafeUtils.ZeroPointer, partKey)
                    }
                    RawChunkSet(ChunkSetInfo.toBytes(info), chunks.toArray)
                  }.toListL.map { rawChunkSets => RawPartData(partKeyBytes, rawChunkSets) }
  }

  val sourceConf = ConfigFactory.parseString("""
    store {
      max-chunks-size = 100
      demand-paged-chunk-retention-period = 10 hours
      shard-mem-size = 50MB
      groups-per-shard = 4
      ingestion-buffer-mem-size = 10MB
      flush-interval = 10 minutes
      part-index-flush-max-delay = 10 seconds
      part-index-flush-min-delay = 2 seconds
    }
  """)

  val storeConf = StoreConfig(sourceConf.getConfig("store"))
  val nativeMem = new NativeMemoryManager(10 * 1024 * 1024)
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

  val firstKey = dataset.timestamp(mapper(names).head)
  val lastKey = dataset.timestamp(mapper(names).last)
  def keyForName(rowNo: Int): Long = dataset.timestamp(mapper(names)(rowNo))

  val partKeyBuilder = new RecordBuilder(TestData.nativeMem, dataset.partKeySchema, 2048)
  val defaultPartKey = partKeyBuilder.addFromObjects(0)

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

  val gdeltLines3 = Source.fromURL(getClass.getResource("/GDELT-sample-test3.csv"))
    .getLines.toSeq.drop(1)     // drop the header line

  val schema = Seq("GLOBALEVENTID:long",
                   "SQLDATE:long",
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

  // Routes input records to the dataset schema correctly
  def records(ds: Dataset, readerSeq: Seq[RowReader] = readers): SomeData = {
    val builder = new RecordBuilder(MemFactory.onHeapFactory, ds.ingestionSchema)
    val routing = ds.ingestRouting(columnNames)
    readerSeq.foreach { row => builder.addFromReader(RoutingRowReader(row, routing)) }
    builder.allContainers.zipWithIndex.map { case (container, i) => SomeData(container, i) }.head
  }

  def dataRows(ds: Dataset, readerSeq: Seq[RowReader] = readers): Seq[RowReader] = {
    val routing = ds.dataRouting(columnNames)
    readerSeq.map { r => RoutingRowReader(r, routing) }
  }

  def partKeyFromRecords(ds: Dataset, records: SomeData, builder: Option[RecordBuilder] = None): Seq[Long] = {
    val partKeyBuilder = builder.getOrElse(new RecordBuilder(TestData.nativeMem, ds.partKeySchema))
    records.records.map { case (base, offset) =>
      ds.comparator.buildPartKeyFromIngest(base, offset, partKeyBuilder)
    }.toVector
  }

  val badLine = ArrayStringRowReader("NotANumber, , , , , , ,".split(','))   // Will fail
  val altLines =
    """0,1979-01-01,197901,1979,AFR,africa,5,5.52631578947368
      |1,1979-01-01,197901,1979,AGR,farm-yo,6,10.9792284866469
      |2,1979-01-01,197901,1979,AGR,farm-yo,6,10.9792284866469""".stripMargin.split("\n")
  val altReaders = altLines.map { line => ArrayStringRowReader(line.split(",")) }

  case class GdeltRecord(eventId: Long, sqlDate: Long, monthYear: Int, year: Int,
                         actor2Code: String, actor2Name: String, numArticles: Int, avgTone: Double)

  val records = gdeltLines.map { line =>
    val parts = line.split(',')
    GdeltRecord(parts(0).toLong, DateTime.parse(parts(1)).getMillis,
                parts(2).toInt, parts(3).toInt,
                parts(4), parts(5), parts(6).toInt, parts(7).toDouble)
  }
  val seqReaders = records.map { record => SeqRowReader(record.productIterator.toList) }

  // Dataset1: Partition keys (Actor2Code, Year) / Row key GLOBALEVENTID
  val dataset1 = Dataset("gdelt", Seq(schema(4), schema(3)), schema.patch(3, Nil, 2), "GLOBALEVENTID")

  // Dataset2: Partition key (MonthYear) / Row keys (GLOBALEVENTID, Actor2Code)
  val dataset2 = Dataset("gdelt", Seq(schema(2)), schema.patch(2, Nil, 1), Seq("GLOBALEVENTID", "Actor2Code"))
  val partBuilder2 = new RecordBuilder(TestData.nativeMem, dataset2.partKeySchema, 10240)

  // Dataset3: same as Dataset1 for now
  val dataset3 = dataset1

  // Dataset4: One big partition (Year) with (Actor2Code, GLOBALEVENTID) rowkey
  // to easily test row key scans
  // val dataset4 = Dataset("gdelt", Seq(schema(3)), schema.patch(3, Nil, 1), Seq("Actor2Code", "GLOBALEVENTID"))
  // val partBuilder4 = new RecordBuilder(TestData.nativeMem, dataset4.partKeySchema, 10240)

  // Proj 6: partition Actor2Code,Actor2Name to test partition key bitmap indexing
  val dataset6 = Dataset("gdelt", schema.slice(4, 6), schema.patch(4, Nil, 2), "GLOBALEVENTID")
}

// A simulation of machine metrics data
object MachineMetricsData {
  import scala.util.Random.nextInt

  val columns = Seq("timestamp:long", "min:double", "avg:double", "max:double", "count:long")

  def singleSeriesData(initTs: Long = System.currentTimeMillis,
                       incr: Long = 1000): Stream[Product] = {
    Stream.from(0).map { n =>
      (Some(initTs + n * incr),
       Some((45 + nextInt(10)).toDouble),
       Some((60 + nextInt(25)).toDouble),
       Some((100 + nextInt(15)).toDouble),
       Some((85 + nextInt(12)).toLong))
    }
  }

  def singleSeriesReaders(): Stream[RowReader] = singleSeriesData().map(TupleRowReader)

  // Dataset1: Partition keys (series) / Row key timestamp
  val dataset1 = Dataset("metrics", Seq("series:string"), columns)
  val partKeyBuilder = new RecordBuilder(TestData.nativeMem, dataset1.partKeySchema, 2048)
  val defaultPartKey = partKeyBuilder.addFromObjects("series0")

  // Turns either multiSeriesData() or linearMultiSeries() into SomeData's for ingestion into MemStore
  def records(ds: Dataset, stream: Stream[Seq[Any]], offset: Int = 0): SomeData = {
    val builder = new RecordBuilder(MemFactory.onHeapFactory, ds.ingestionSchema)
    stream.foreach { row =>
      builder.startNewRecord()
      row.foreach { thing => builder.addSlowly(thing) }
      builder.endRecord()
    }
    builder.allContainers.zipWithIndex.map { case (container, i) => SomeData(container, i + offset) }.head
  }

  def groupedRecords(ds: Dataset, stream: Stream[Seq[Any]], n: Int = 100, groupSize: Int = 5): Seq[SomeData] =
    stream.take(n).grouped(groupSize).toSeq.zipWithIndex.map { case (group, i) => records(ds, group, i) }

  // Takes the partition key from stream record n, filtering the stream by only that partition,
  // then creates a ChunkSetStream out of it
  // Works with linearMultiSeries() or multiSeriesData()
  def filterByPartAndMakeStream(stream: Stream[Seq[Any]], keyRecord: Int): Observable[ChunkSet] = {
    val rawPartKey = stream(keyRecord)(5)
    val partKey = partKeyBuilder.addFromObjects(rawPartKey)
    TestData.toChunkSetStream(dataset1, partKey, stream.filter(_(5) == rawPartKey).map(SeqRowReader))
  }

  def multiSeriesData(): Stream[Seq[Any]] = {
    val initTs = System.currentTimeMillis
    Stream.from(0).map { n =>
      Seq(initTs + n * 1000,
         (45 + nextInt(10)).toDouble,
         (60 + nextInt(25)).toDouble,
         (99.9 + nextInt(15)).toDouble,
         (85 + nextInt(12)).toLong,
         "Series " + (n % 10))
    }
  }

  // Everything increments by 1 for simple predictability and testing
  def linearMultiSeries(startTs: Long = 100000L, numSeries: Int = 10, timeStep: Int = 1000): Stream[Seq[Any]] = {
    Stream.from(0).map { n =>
      Seq(startTs + n * timeStep,
         (1 + n).toDouble,
         (20 + n).toDouble,
         (99.9 + n).toDouble,
         (85 + n).toLong,
         "Series " + (n % numSeries))
    }
  }

  def addToBuilder(builder: RecordBuilder, data: Seq[Seq[Any]]): Unit = {
    data.foreach { values =>
      builder.startNewRecord()
      builder.addLong(values(0).asInstanceOf[Long])     // timestamp
      builder.addDouble(values(1).asInstanceOf[Double])  // min
      builder.addDouble(values(2).asInstanceOf[Double])  // avg
      builder.addDouble(values(3).asInstanceOf[Double]) // max
      builder.addLong(values(4).asInstanceOf[Long])  // count
      builder.addString(values(5).asInstanceOf[String])  // series (partition key)

      if (values.length > 6) {
        builder.startMap()
        values(6).asInstanceOf[UTF8Map].toSeq.sortBy(_._1).foreach { case (k, v) =>
          builder.addMapKeyValue(k.bytes, v.bytes)
        }
        builder.endMap()
      }
      builder.endRecord()
    }
  }

  val dataset2 = Dataset("metrics", Seq("series:string", "tags:map"), columns)

  def withMap(data: Stream[Seq[Any]], n: Int = 5, extraTags: UTF8Map = Map.empty): Stream[Seq[Any]] =
    data.zipWithIndex.map { case (row, idx) => row :+ (Map("n".utf8 -> (idx % n).toString.utf8) ++ extraTags) }

  val uuidString = java.util.UUID.randomUUID.toString
  val extraTags = Map("job".utf8 -> "prometheus".utf8,
                      "cloudProvider".utf8 -> "AmazonAWS".utf8,
                      "region".utf8 -> "AWS-USWest".utf8,
                      "instance".utf8 -> uuidString.utf8)

  val tagsWithDiffLen = extraTags ++ Map("job".utf8 -> "prometheus23".utf8)
  val tagsDiffSameLen = extraTags ++ Map("region".utf8 -> "AWS-USEast".utf8)

  val extraTagsLen = extraTags.map { case (k, v) => k.numBytes + v.numBytes }.sum
}

object MetricsTestData {
  val timeseriesDataset = Dataset.make("timeseries",
                                  Seq("tags:map"),
                                  Seq("timestamp:ts", "value:double"),
                                  Seq("timestamp"),
                                  DatasetOptions(Seq("__name__", "job"), "__name__", "value")).get
  val builder = new RecordBuilder(MemFactory.onHeapFactory, timeseriesDataset.ingestionSchema)

  final case class TagsRowReader(tags: Map[String, String]) extends SchemaRowReader {
    val extractors = Array[RowReader.TypedFieldExtractor[_]](ColumnType.MapColumn.keyType.extractor)
    val kvMap = tags.map { case (k, v) => ZeroCopyUTF8String(k) -> ZeroCopyUTF8String(v) }
    final def getDouble(index: Int): Double = ???
    final def getLong(index: Int): Long = ???
    final def getString(index: Int): String = ???
    final def getAny(index: Int): Any = kvMap
    final def getBoolean(columnNo: Int): Boolean = ???
    final def getFloat(columnNo: Int): Float = ???
    final def getInt(columnNo: Int): Int = ???
    final def notNull(columnNo: Int): Boolean = true
    final def getBlobBase(columnNo: Int): Any = ???
    final def getBlobOffset(columnNo: Int): PartitionKey = ???
    final def getBlobNumBytes(columnNo: Int): Int = ???
  }

}
