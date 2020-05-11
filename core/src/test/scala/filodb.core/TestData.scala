package filodb.core

import scala.concurrent.duration._
import scala.io.Source

import com.typesafe.config.ConfigFactory
import monix.eval.Task
import monix.reactive.Observable
import org.joda.time.DateTime

import filodb.core.Types.{PartitionKey, UTF8Map}
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.memstore.{SomeData, TimeSeriesPartitionSpec, WriteBufferPool}
import filodb.core.metadata.{Dataset, DatasetOptions, Schema, Schemas}
import filodb.core.metadata.Column.ColumnType
import filodb.core.query.RawDataRangeVector
import filodb.core.store._
import filodb.memory._
import filodb.memory.format.{vectors => bv, _}
import filodb.memory.format.ZeroCopyUTF8String._

object TestData {
  def toChunkSetStream(s: Schema,
                       part: PartitionKey,
                       rows: Seq[RowReader],
                       rowsPerChunk: Int = 10): Observable[ChunkSet] =
    Observable.fromIterator(rows.grouped(rowsPerChunk).map {
      chunkRows => ChunkSet(s.data, part, 0, chunkRows, nativeMem) })

  def toRawPartData(chunkSetStream: Observable[ChunkSet]): Task[RawPartData] = {
    var partKeyBytes: Array[Byte] = null
    chunkSetStream.map { case ChunkSet(info, partKey, _, chunks, _) =>
                    if (partKeyBytes == null) {
                      partKeyBytes = BinaryRegionLarge.asNewByteArray(UnsafeUtils.ZeroPointer, partKey)
                    }
                    RawChunkSet(ChunkSetInfo.toBytes(info), chunks.toArray)
                  }.toListL.map { rawChunkSets => RawPartData(partKeyBytes, rawChunkSets) }
  }

  val sourceConfStr = """
    store {
      max-chunks-size = 100
      demand-paged-chunk-retention-period = 10 hours
      shard-mem-size = 100MB
      groups-per-shard = 4
      ingestion-buffer-mem-size = 80MB
      max-buffer-pool-size = 250
      flush-interval = 10 minutes
      part-index-flush-max-delay = 10 seconds
      part-index-flush-min-delay = 2 seconds
    }
  """
  val sourceConf = ConfigFactory.parseString(sourceConfStr)

  val storeConf = StoreConfig(sourceConf.getConfig("store"))
  val nativeMem = new NativeMemoryManager(50 * 1024 * 1024)

  val optionsString = """
  options {
    copyTags = {}
    ignoreShardKeyColumnSuffixes = {}
    ignoreTagsOnPartitionKeyHash = ["le"]
    metricColumn = "__name__"
    valueColumn = "value"
    shardKeyColumns = ["__name__", "_ns_", "_ws_"]
  }
  """
}

object NamesTestData {
  def mapper(rows: Seq[Product]): Seq[RowReader] = rows.map(TupleRowReader)

  val dataColSpecs = Seq("age:long:interval=10", "first:string", "last:string")
  val dataset = Dataset("dataset", Seq("seg:int"), dataColSpecs, DatasetOptions.DefaultOptions)
  val schema = dataset.schema

  // NOTE: first 3 columns are the data columns, thus names could be used for either complete record
  // or the data column rowReader
  val names = Seq((Some(24L), Some("Khalil"),    Some("Mack"),     Some(0)),
                  (Some(28L), Some("Ndamukong"), Some("Suh"),      Some(0)),
                  (Some(25L), Some("Rodney"),    Some("Hudson"),   Some(0)),
                  (Some(40L), Some("Jerry"),     None,             Some(0)),
                  (Some(39L), Some("Peyton"),    Some("Manning"),  Some(0)),
                  (Some(29L), Some("Terrance"),  Some("Knighton"), Some(0)))

  val altNames = Seq((Some(24L), Some("Stacy"),    Some("McGee"),     Some(0)),
                     (Some(28L), Some("Bruce"),     Some("Irvin"),    Some(0)),
                     (Some(25L), Some("Amari"),     Some("Cooper"),   Some(0)),
                     (Some(40L), Some("Jerry"),     None,             Some(0)),
                     (Some(39L), Some("Derek"),     Some("Carr"),     Some(0)),
                     (Some(29L), Some("Karl"),      Some("Joseph"),   Some(0)))

  val firstKey = schema.timestamp(mapper(names).head)
  val lastKey = schema.timestamp(mapper(names).last)
  def keyForName(rowNo: Int): Long = schema.timestamp(mapper(names)(rowNo))

  val partKeyBuilder = new RecordBuilder(TestData.nativeMem, 2048)
  val defaultPartKey = partKeyBuilder.partKeyFromObjects(dataset.schema, 0)

  def chunkSetStream(data: Seq[Product] = names): Observable[ChunkSet] =
    TestData.toChunkSetStream(schema, defaultPartKey, mapper(data))

  val firstNames = names.map(_._2.get)
  val utf8FirstNames = firstNames.map(_.utf8)
  val sortedFirstNames = Seq("Khalil", "Rodney", "Ndamukong", "Terrance", "Peyton", "Jerry")
  val sortedUtf8Firsts = sortedFirstNames.map(_.utf8)

  val largeDataset = Dataset("dataset", Seq("league:string"), dataColSpecs, DatasetOptions.DefaultOptions)

  val lotLotNames = {
    for { league <- Seq("nfc", "afc")
          numChunks <- 0 to 2
          chunk  <- 0 to numChunks
          startRowNo = numChunks * 10000 + chunk * 100
          rowNo  <- startRowNo to (startRowNo + 99) }
    yield { (Some(rowNo.toLong),             // the unique row key
             names(rowNo % 6)._1, names(rowNo % 6)._2,
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
    val builder = new RecordBuilder(MemFactory.onHeapFactory)
    val routing = ds.schema.ingestRouting(columnNames)
    readerSeq.foreach { row => builder.addFromReader(RoutingRowReader(row, routing), ds.schema) }
    builder.allContainers.zipWithIndex.map { case (container, i) => SomeData(container, i) }.head
  }

  def dataRows(ds: Dataset, readerSeq: Seq[RowReader] = readers): Seq[RowReader] = {
    val routing = ds.schema.dataRouting(columnNames)
    readerSeq.map { r => RoutingRowReader(r, routing) }
  }

  def partKeyFromRecords(ds: Dataset, records: SomeData, builder: Option[RecordBuilder] = None): Seq[Long] = {
    val partKeyBuilder = builder.getOrElse(new RecordBuilder(TestData.nativeMem))
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

  // NOTE: For all datasets the row key is GLOBALEVENTID
  // Dataset1: Partition keys (Actor2Code, Year)
  val dataset1 = Dataset("gdelt", Seq(schema(4), schema(3)), schema.patch(3, Nil, 2), DatasetOptions.DefaultOptions)
  val schema1 = dataset1.schema

  // Dataset2: Partition key (MonthYear)
  val dataset2 = Dataset("gdelt", Seq(schema(2)), schema.patch(2, Nil, 1))
  val schema2 = dataset2.schema
  val partBuilder2 = new RecordBuilder(TestData.nativeMem, 10240)

  // Dataset3: same as Dataset1 for now
  val dataset3 = dataset1

  // Proj 6: partition Actor2Code,Actor2Name to test partition key bitmap indexing
  val datasetOptions = DatasetOptions.DefaultOptions.copy(
    shardKeyColumns = Seq( "__name__","_ns_","_ws_"))
  val dataset6 = Dataset("gdelt", schema.slice(4, 6), schema.patch(4, Nil, 2), datasetOptions)

  val datasetOptionConfig = """
    options {
      shardKeyColumns = ["__name__","_ns_","_ws_"]
      ignoreShardKeyColumnSuffixes = {}
      valueColumn = "AvgTone"
      metricColumn = "__name__"
      ignoreTagsOnPartitionKeyHash = []
      copyTags = {}
    }
  """
}

// A simulation of machine metrics data, all with the same partition key.  Forms a set of schemas.
object MachineMetricsData {
  import scala.util.Random.nextInt

  val columns = Seq("timestamp:ts", "min:double", "avg:double", "max:double", "count:long")
  val dummyContext = Map("test" -> "test")

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
  val options = DatasetOptions.DefaultOptions.copy(metricColumn = "series")
  val dataset1 = Dataset("metrics1", Seq("series:string"), columns, options)
  val schema1 = dataset1.schema
  val partKeyBuilder = new RecordBuilder(TestData.nativeMem, 2048)
  val defaultPartKey = partKeyBuilder.partKeyFromObjects(dataset1.schema, "series0")

  // Turns either multiSeriesData() or linearMultiSeries() into SomeData's for ingestion into MemStore
  def records(ds: Dataset, stream: Stream[Seq[Any]], offset: Int = 0,
              ingestionTimeMillis: Long = System.currentTimeMillis()): SomeData = {
    val builder = new RecordBuilder(MemFactory.onHeapFactory) {
      override def currentTimeMillis: Long = ingestionTimeMillis
    }
    stream.foreach { row =>
      builder.startNewRecord(ds.schema)
      row.foreach { thing => builder.addSlowly(thing) }
      builder.endRecord()
    }
    builder.allContainers.zipWithIndex.map { case (container, i) => SomeData(container, i + offset) }.head
  }

  /**
    * @param ingestionTimeStep defines the ingestion time increment for each generated
    * RecordContainer
    */
  def groupedRecords(ds: Dataset, stream: Stream[Seq[Any]], n: Int = 100, groupSize: Int = 5,
                     ingestionTimeStep: Long = 40000, ingestionTimeStart: Long = 0,
                     offset: Int = 0): Seq[SomeData] =
    stream.take(n).grouped(groupSize).toSeq.zipWithIndex.map {
      case (group, i) => records(ds, group, offset + i, ingestionTimeStart + i * ingestionTimeStep)
    }

  // Takes the partition key from stream record n, filtering the stream by only that partition,
  // then creates a ChunkSetStream out of it
  // Works with linearMultiSeries() or multiSeriesData()
  def filterByPartAndMakeStream(stream: Stream[Seq[Any]], keyRecord: Int): Observable[ChunkSet] = {
    val rawPartKey = stream(keyRecord)(5)
    val partKey = partKeyBuilder.partKeyFromObjects(schema1, rawPartKey)
    TestData.toChunkSetStream(schema1, partKey, stream.filter(_(5) == rawPartKey).map(SeqRowReader))
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
  def linearMultiSeries(startTs: Long = 100000L, numSeries: Int = 10, timeStep: Int = 1000,
                        seriesPrefix: String = "Series "): Stream[Seq[Any]] = {
    Stream.from(0).map { n =>
      Seq(startTs + n * timeStep,
         (1 + n).toDouble,
         (20 + n).toDouble,
         (99.9 + n).toDouble,
         (85 + n).toLong,
         seriesPrefix + (n % numSeries))
    }
  }

  def addToBuilder(builder: RecordBuilder, data: Seq[Seq[Any]], schema: Schema = dataset1.schema): Unit = {
    data.foreach { values =>
      builder.startNewRecord(schema)
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

  val dataset2 = Dataset("metrics2", Seq("series:string", "tags:map"), columns)
  val schema2 = dataset2.schema

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

  val histDataset = Dataset("histogram", Seq("metric:string", "tags:map"),
                            Seq("timestamp:ts", "count:long", "sum:long", "h:hist:counter=false"),
                            DatasetOptions.DefaultOptions.copy(metricColumn = "metric"))

  var histBucketScheme: bv.HistogramBuckets = _
  def linearHistSeries(startTs: Long = 100000L, numSeries: Int = 10, timeStep: Int = 1000, numBuckets: Int = 8,
                       infBucket: Boolean = false):
  Stream[Seq[Any]] = {
    val scheme = if (infBucket) {
                   // Custom geometric buckets, with top bucket being +Inf
                   val buckets = (0 to numBuckets - 2).map(n => Math.pow(2.0, n + 1)) ++ Seq(Double.PositiveInfinity)
                   bv.CustomBuckets(buckets.toArray)
                 } else bv.GeometricBuckets(2.0, 2.0, numBuckets)
    histBucketScheme = scheme
    val buckets = new Array[Long](numBuckets)
    def updateBuckets(bucketNo: Int): Unit = {
      for { b <- bucketNo until numBuckets } {
        buckets(b) += 1
      }
    }
    Stream.from(0).map { n =>
      updateBuckets(n % numBuckets)
      Seq(startTs + n * timeStep,
          (1 + n).toLong,
          buckets.sum.toLong,
          bv.LongHistogram(scheme, buckets.map(x => x)),
          "request-latency",
          extraTags ++ Map("_ws_".utf8 -> "demo".utf8, "_ns_".utf8 -> "testapp".utf8, "dc".utf8 -> s"${n % numSeries}".utf8))
    }
  }

  // Data usable with prom-histogram schema
  def linearPromHistSeries(startTs: Long = 100000L, numSeries: Int = 10, timeStep: Int = 1000, numBuckets: Int = 8):
  Stream[Seq[Any]] = linearHistSeries(startTs, numSeries, timeStep, numBuckets).map { d =>
    d.updated(1, d(1).asInstanceOf[Long].toDouble).updated(2, d(2).asInstanceOf[Long].toDouble)
  }

  // dataset2 + histDataset
  val schemas2h = Schemas(schema2.partition,
                        Map(schema2.name -> schema2, "histogram" -> histDataset.schema))

  val histMaxDS = Dataset("histmax", Seq("metric:string", "tags:map"),
                          Seq("timestamp:ts", "count:long", "sum:long", "max:double", "h:hist:counter=false"))

  // Pass in the output of linearHistSeries here.
  // Adds in the max column before h/hist
  def histMax(histStream: Stream[Seq[Any]]): Stream[Seq[Any]] =
    histStream.map { row =>
      val hist = row(3).asInstanceOf[bv.LongHistogram]
      // Set max to a fixed ratio of the "last bucket" top value, ie the last bucket with an actual increase
      val highestBucketVal = hist.bucketValue(hist.numBuckets - 1)
      val lastBucketNum = ((hist.numBuckets - 2) to 0 by -1).filter { b => hist.bucketValue(b) == highestBucketVal }
                            .lastOption.getOrElse(hist.numBuckets - 1)
      val max = hist.bucketTop(lastBucketNum) * 0.8
      ((row take 3) :+ max) ++ (row drop 3)
    }

  val histKeyBuilder = new RecordBuilder(TestData.nativeMem, 2048)
  val histPartKey = histKeyBuilder.partKeyFromObjects(histDataset.schema, "request-latency", extraTags)

  val blockStore = new PageAlignedBlockManager(100 * 1024 * 1024, new MemoryStats(Map("test"-> "test")), null, 16)
  val histIngestBH = new BlockMemFactory(blockStore, false, histDataset.schema.data.blockMetaSize,
                                         dummyContext, true)
  val histMaxBH = new BlockMemFactory(blockStore, false, histMaxDS.schema.data.blockMetaSize,
                                      dummyContext, true)
  private val histBufferPool = new WriteBufferPool(TestData.nativeMem, histDataset.schema.data, TestData.storeConf)

  // Designed explicitly to work with linearHistSeries records and histDataset from MachineMetricsData
  def histogramRV(startTS: Long, pubFreq: Long = 10000L, numSamples: Int = 100, numBuckets: Int = 8,
                  infBucket: Boolean = false, ds: Dataset = histDataset, pool: WriteBufferPool = histBufferPool):
  (Stream[Seq[Any]], RawDataRangeVector) = {
    val histData = linearHistSeries(startTS, 1, pubFreq.toInt, numBuckets, infBucket).take(numSamples)
    val container = records(ds, histData).records
    val part = TimeSeriesPartitionSpec.makePart(0, ds, partKey=histPartKey, bufferPool=pool)
    container.iterate(ds.ingestionSchema).foreach { row => part.ingest(0, row, histIngestBH, 1.hour.toMillis) }
    // Now flush and ingest the rest to ensure two separate chunks
    part.switchBuffers(histIngestBH, encode = true)
    (histData, RawDataRangeVector(null, part, AllChunkScan, Array(0, 3)))  // select timestamp and histogram columns only
  }

  private val histMaxBP = new WriteBufferPool(TestData.nativeMem, histMaxDS.schema.data, TestData.storeConf)

  // Designed explicitly to work with histMax(linearHistSeries) records
  def histMaxRV(startTS: Long, pubFreq: Long = 10000L, numSamples: Int = 100, numBuckets: Int = 8):
  (Stream[Seq[Any]], RawDataRangeVector) = {
    val histData = histMax(linearHistSeries(startTS, 1, pubFreq.toInt, numBuckets)).take(numSamples)
    val container = records(histMaxDS, histData).records
    val part = TimeSeriesPartitionSpec.makePart(0, histMaxDS, partKey=histPartKey, bufferPool=histMaxBP)
    container.iterate(histMaxDS.ingestionSchema).foreach { row => part.ingest(0, row, histMaxBH, 1.hour.toMillis) }
    // Now flush and ingest the rest to ensure two separate chunks
    part.switchBuffers(histMaxBH, encode = true)
    // Select timestamp, hist, max
    (histData, RawDataRangeVector(null, part, AllChunkScan, Array(0, 4, 3)))
  }
}

// A simulation of custom machine metrics data - for testing extractTimeBucket
object CustomMetricsData {
  val columns = Seq("timestamp:ts", "min:double", "avg:double", "max:double", "count:long")

  //Partition Key with multiple string columns
  val partitionColumns = Seq("_metric_:string", "app:string")
  val metricdataset = Dataset.make("tsdbdata",
                        partitionColumns,
                        columns,
                        Seq.empty,
                        None,
                        DatasetOptions(Seq("_metric_", "_ns_", "_ws_"), "_metric_", true)).get
  val partKeyBuilder = new RecordBuilder(TestData.nativeMem, 2048)
  val defaultPartKey = partKeyBuilder.partKeyFromObjects(metricdataset.schema, "metric1", "app1")

  //Partition Key with single map columns
  val partitionColumns2 = Seq("tags:map")
  val metricdataset2 = Dataset.make("tsdbdata",
                        partitionColumns2,
                        columns,
                        Seq.empty,
                        None,
                        DatasetOptions(Seq("__name__"), "__name__", true)).get
  val partKeyBuilder2 = new RecordBuilder(TestData.nativeMem, 2048)
  val defaultPartKey2 = partKeyBuilder2.partKeyFromObjects(metricdataset2.schema,
                                                           Map(ZeroCopyUTF8String("abc") -> ZeroCopyUTF8String("cba")))

}

object MetricsTestData {
  val timeseriesDataset = Dataset.make("timeseries",
                                  Seq("tags:map"),
                                  Seq("timestamp:ts", "value:double:detectDrops=true"),
                                  Seq.empty,
                                  None,
                                  DatasetOptions(Seq("__name__", "job"), "__name__")).get
  val timeseriesSchema = timeseriesDataset.schema
  val timeseriesSchemas = Schemas(timeseriesSchema)

  val timeseriesDatasetWithMetric = Dataset.make("timeseries",
    Seq("tags:map"),
    Seq("timestamp:ts", "value:double:detectDrops=true"),
    Seq.empty,
    None,
    DatasetOptions(Seq("_metric_", "_ns_"), "_metric_")).get

  val downsampleDataset = Dataset.make("tsdbdata",
    Seq("tags:map"),
    Seq("timestamp:ts", "min:double", "max:double", "sum:double", "count:double", "avg:double"),
    Seq.empty,
    options = DatasetOptions(Seq("__name__"), "__name__", true)
  ).get
  val downsampleSchema = downsampleDataset.schema

  val builder = new RecordBuilder(MemFactory.onHeapFactory)

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

  // Takes the output of linearHistSeries and transforms them into Prometheus-schema histograms.
  // Each bucket becomes its own time series with _bucket appended and an le value
  def promHistSeries(startTs: Long = 100000L, numSeries: Int = 10, timeStep: Int = 1000, numBuckets: Int = 8):
  Stream[Seq[Any]] =
    MachineMetricsData.linearHistSeries(startTs, numSeries, timeStep, numBuckets)
      .flatMap { record =>
        val timestamp = record(0)
        val tags = record(5).asInstanceOf[Map[ZeroCopyUTF8String, ZeroCopyUTF8String]]
        val metricName = record(4).toString
        val countRec = Seq(timestamp, record(1).asInstanceOf[Long].toDouble,
                           tags + ("__name__".utf8 -> (metricName + "_count").utf8))
        val sumRec = Seq(timestamp, record(2).asInstanceOf[Long].toDouble,
                           tags + ("__name__".utf8 -> (metricName + "_sum").utf8))
        val hist = record(3).asInstanceOf[bv.MutableHistogram]
        val bucketTags = tags + ("__name__".utf8 -> (metricName + "_bucket").utf8)
        val bucketRecs = (0 until hist.numBuckets).map { b =>
          Seq(timestamp, hist.bucketValue(b),
              bucketTags + ("le".utf8 -> hist.bucketTop(b).toString.utf8))
        }
        Seq(countRec, sumRec) ++ bucketRecs
      }
}
