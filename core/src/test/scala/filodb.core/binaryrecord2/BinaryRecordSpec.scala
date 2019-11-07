package filodb.core.binaryrecord2

import debox.Buffer
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSpec, Matchers}

import filodb.core.{MachineMetricsData, Types}
import filodb.core.metadata.Column.ColumnType
import filodb.core.metadata.{Dataset, DatasetOptions}
import filodb.core.query.ColumnInfo
import filodb.memory._
import filodb.memory.format.{SeqRowReader, UnsafeUtils, ZeroCopyUTF8String => ZCUTF8}

class BinaryRecordSpec extends FunSpec with Matchers with BeforeAndAfter with BeforeAndAfterAll {
  import MachineMetricsData._
  import UTF8StringMedium._
  import RecordBuilder.ContainerHeaderLen

  val recSchema1 = schema1.ingestionSchema
  val recSchema2 = schema2.ingestionSchema
  val longStrSchema = new RecordSchema(Seq(ColumnInfo("lc", ColumnType.LongColumn),
                                           ColumnInfo("sc", ColumnType.StringColumn)))

  val records = new collection.mutable.ArrayBuffer[(Any, Long)]

  import com.softwaremill.quicklens._

  val dataset3 = modify(dataset2)(_.schema.partition.predefinedKeys).setTo(Seq("job", "instance"))
  val schemaWithPredefKeys = dataset3.schema.ingestionSchema

  before {
    records.clear()
  }

  val consumer = new BinaryRegionConsumer {
    def onNext(base: Any, offset: Long): Unit = records += ((base, offset))
  }

  val nativeMem = new NativeMemoryManager(10 * 1024 * 1024)

  override def afterAll(): Unit = {
    nativeMem.shutdown()   // free all memory
  }


  describe("RecordBuilder & RecordContainer") {
    it("should not allow adding a field before startNewRecord()") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)
      intercept[IllegalArgumentException] {
        builder.addInt(1)
      }
    }

    it("should not allow adding a field beyond # of fields in schema") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)
      builder.startNewRecord(schema1)
      val ts = System.currentTimeMillis
      builder.addLong(ts)     // timestamp
      builder.addDouble(1.0)  // min
      builder.addDouble(2.5)  // avg
      builder.addDouble(10.1) // max
      builder.addLong(123456L)  // count
      builder.addString("Series 1")   // series (partition key)

      intercept[IllegalArgumentException] {
        builder.addInt(1234)   // beyond last field
      }
    }

    it("should not allow adding a string longer than 64KB...") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)
      builder.startNewRecord(schema1)
      val ts = System.currentTimeMillis
      builder.addLong(ts)     // timestamp
      builder.addDouble(1.0)  // min
      builder.addDouble(2.5)  // avg
      builder.addDouble(10.1) // max
      builder.addLong(123456L)  // count

      intercept[IllegalArgumentException] {
        builder.addString("ABCDEfghij" * 7000)
      }
    }

    it("should not write hash if schema does not have partition key") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)
      val sourceRow = SeqRowReader(Seq(10000L, "ABCDEfghij"))
      builder.addFromReader(sourceRow, longStrSchema, 2345)

      builder.allContainers should have length (1)
      builder.allContainers.head.iterate(longStrSchema).foreach { row =>
        row.getLong(0) shouldEqual 10000L
        row.filoUTF8String(1).toString shouldEqual "ABCDEfghij"
      }
    }

    // 62 bytes per record, rounded to 64.  2048 max, what are the number of records we can have?
    val maxNumRecords = (RecordBuilder.MinContainerSize - ContainerHeaderLen) / 64
    val remainingBytes = RecordBuilder.MinContainerSize - ContainerHeaderLen - maxNumRecords*64

    it("should add multiple records, return offsets, and roll over record to new container if needed") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory, RecordBuilder.MinContainerSize)

      val data = linearMultiSeries().take(maxNumRecords + 1)
      addToBuilder(builder, data take maxNumRecords)

      // At this point get offsets, we should have one container only
      builder.allContainers should have length (1)
      builder.containerRemaining shouldEqual remainingBytes
      builder.allContainers.head.consumeRecords(consumer)
      records should have length (maxNumRecords)
      builder.allContainers.head.countRecords() shouldEqual maxNumRecords
      builder.allContainers.head.isEmpty shouldEqual false

      // should all have the same base
      records.map(_._1).forall(_ == records.head._1)
      // check min value
      records.map { case (b, o) => schema1.ingestionSchema.getDouble(b, o, 1) } shouldEqual
        (1 to maxNumRecords).map(_.toDouble)
      records.foreach { case (b, o) =>
        schema1.ingestionSchema.partitionHash(b, o) should not be (0)
        RecordSchema.schemaID(b, o) shouldEqual schema1.schemaHash
      }
      val container1Bytes = builder.allContainers.head.numBytes

      // Ok, now add one more record. With only 60 bytes remaining, we should start over again in 2nd container.
      // bytes remaining in first container should be the same, 2nd container will have 64 bytes (62 rounded up)
      addToBuilder(builder, data drop maxNumRecords)
      val containers = builder.allContainers
      containers should have length (2)
      containers.head.numBytes shouldEqual container1Bytes
      containers.last.numBytes shouldEqual 76

      containers.last.countRecords shouldEqual 1

      containers.foreach(_.version shouldEqual RecordBuilder.Version)
      containers.foreach(_.isCurrentVersion shouldEqual true)
      containers.foreach(_.timestamp should be > 0L)

      builder.nonCurrentContainerBytes().map(_.size) shouldEqual Seq(RecordBuilder.MinContainerSize)
      builder.optimalContainerBytes(true).map(_.size) shouldEqual Seq(RecordBuilder.MinContainerSize, 80)
      builder.nonCurrentContainerBytes().size shouldEqual 0
      builder.optimalContainerBytes().size shouldEqual 0

    }

    it("should add multiple records and rollover for offheap containers") {
      val builder = new RecordBuilder(nativeMem, RecordBuilder.MinContainerSize)

      val data = linearMultiSeries().take(maxNumRecords + 1)
      addToBuilder(builder, data take maxNumRecords)

      // At this point get offsets, we should have one container only
      builder.allContainers should have length (1)
      builder.containerRemaining shouldEqual remainingBytes
      val addrs = builder.allContainers.head.allOffsets
      addrs should have length (maxNumRecords)
      // check min value
      addrs.map(recSchema1.getDouble(_, 1)) shouldEqual Buffer.fromIterable((1 to maxNumRecords).map(_.toDouble))
      addrs.foreach { a =>
        recSchema1.partitionHash(a) should not be (0)
        RecordSchema.schemaID(a) shouldEqual schema1.schemaHash
      }
      val container1Bytes = builder.allContainers.head.numBytes

      // Ok, now add one more record. With only 60 bytes remaining, we should start over again in 2nd container.
      // bytes remaining in first container should be the same, 2nd container will have 62 bytes
      addToBuilder(builder, data drop maxNumRecords)
      val containers = builder.allContainers
      containers should have length (2)
      containers.head.numBytes shouldEqual container1Bytes
      containers.last.numBytes shouldEqual 76

      // Cannot get byte array via optimalContainerBytes for offheap containers
      intercept[UnsupportedOperationException] {
        builder.optimalContainerBytes()
      }

      records.clear()
      containers.last.consumeRecords(consumer)
      records should have length (1)
    }

    it("should add multiple records, return offsets, and rollover to same container if reuseOneContainer=true") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory, RecordBuilder.MinContainerSize,
                                      reuseOneContainer=true)
      // when reuseOneContainer = true, one container should be allocated on startup
      builder.allContainers should have length(1)
      builder.allContainers.head.base should not equal (null)

      val data = linearMultiSeries().take(maxNumRecords + 1)
      addToBuilder(builder, data take maxNumRecords)

      // At this point get offsets, we should have one container only
      builder.allContainers should have length (1)
      builder.containerRemaining shouldEqual remainingBytes
      builder.allContainers.head.consumeRecords(consumer)
      records should have length (maxNumRecords)
      builder.allContainers.head.countRecords() shouldEqual maxNumRecords
      builder.allContainers.head.isEmpty shouldEqual false

      // Ok, now add one more record. With only 60 bytes remaining, we should reset and
      // copy part of record just added to beginning of container.
      addToBuilder(builder, data drop maxNumRecords)
      val containers = builder.allContainers
      containers should have length (1)
      containers.head.numBytes shouldEqual 76
      containers.last.countRecords shouldEqual 1
    }

    it("should add records, reset, and be able to add records again") {
      val builder = new RecordBuilder(nativeMem, RecordBuilder.MinContainerSize)
      addToBuilder(builder, linearMultiSeries() take 10)
      val curTime = System.currentTimeMillis

      // Now check amount of space left in container, container bytes etc
      builder.allContainers should have length (1)
      builder.allContainers.head.numBytes shouldEqual (12 + 64*10)
      builder.allContainers.head.isEmpty shouldEqual false
      builder.currentContainer.get.isEmpty shouldEqual false
      builder.allContainers.head.countRecords shouldEqual 10
      val origTimestamp = builder.allContainers.head.timestamp
      origTimestamp shouldEqual curTime +- 20000   // 20 seconds

      val byteArrays = builder.optimalContainerBytes(reset = true)
      byteArrays.size shouldEqual 1

      // Check that we still have one container but it's empty
      builder.allContainers should have length (1)
      builder.allContainers.head.numBytes shouldEqual 12
      builder.allContainers.head.isEmpty shouldEqual true
      builder.allContainers.head.timestamp should be > origTimestamp
      builder.currentContainer.get.isEmpty shouldEqual true

      // Add some more records
      // CHeck amount of space left, should be same as before
      addToBuilder(builder, linearMultiSeries() take 9)
      builder.allContainers should have length (1)
      builder.allContainers.head.numBytes shouldEqual (12 + 64*9)
      builder.allContainers.head.countRecords shouldEqual 9
    }

    it("should add records and iterate") {
      val builder = new RecordBuilder(nativeMem, RecordBuilder.MinContainerSize)
      val data = linearMultiSeries() take 10
      addToBuilder(builder, data)

      // Now check amount of space left in container, container bytes etc
      builder.allContainers should have length (1)
      builder.allContainers.head.numBytes shouldEqual (12 + 64*10)
      builder.allContainers.head.countRecords shouldEqual 10

      val it = builder.allContainers.head.iterate(recSchema1)
      val doubles = data.map(_(1).asInstanceOf[Double])
      it.map(_.getDouble(1)).toBuffer shouldEqual doubles
    }
  }

  import UTF8StringShort._
  val sortedKeys = Seq("cloudProvider", "instance", "job", "n", "region").map(_.utf8short(nativeMem))
  var lastIndex = -1

  val keyCheckConsumer = new MapItemConsumer {
    def consume(keyBase: Any, keyOffset: Long, valueBase: Any, valueOffset: Long, index: Int): Unit = {
      // println(s"XXX: got key, ${UTF8StringMedium.toString(keyBase, keyOffset)}")
      lastIndex = index
      UTF8StringShort.equals(keyBase, keyOffset, null, sortedKeys(index)) shouldEqual true
    }
  }

  val sortedValues = Seq("AmazonAWS", uuidString, "prometheus", "0", "AWS-USWest").map(_.utf8(nativeMem))

  val valuesCheckConsumer = new MapItemConsumer {
    def consume(keyBase: Any, keyOffset: Long, valueBase: Any, valueOffset: Long, index: Int): Unit = {
      UTF8StringMedium.equals(valueBase, valueOffset, null, sortedValues(index).address) shouldEqual true
    }
  }

  private def align4(n: Int) = (n + 3) & ~3

  describe("RecordSchema & BinaryRecord") {
    it("should add and get double, long, UTF8 fields correctly") {
      val builder = new RecordBuilder(nativeMem)
      builder.startNewRecord(schema1)
      val ts = System.currentTimeMillis
      builder.addLong(ts)     // timestamp
      builder.addDouble(1.0)  // min
      builder.addDouble(2.5)  // avg
      builder.addDouble(10.1) // max
      builder.addLong(123456L)  // count
      builder.addString("Series 1")   // series (partition key)
      val offset1 = builder.endRecord()

      // now test everything
      val containers = builder.allContainers
      containers should have length (1)
      containers.head.numBytes shouldEqual 76   // versionWord + ts + len + long + 4 doubles + stringptr + hash + 2+8 + padding

      containers.head.consumeRecords(consumer)
      records should have length (1)
      val recordAddr = records.head._2
      recSchema1.getLong(recordAddr, 0) shouldEqual ts
      recSchema1.getDouble(recordAddr, 1) shouldEqual 1.0
      recSchema1.getDouble(recordAddr, 2) shouldEqual 2.5
      recSchema1.getDouble(recordAddr, 3) shouldEqual 10.1
      recSchema1.getLong(recordAddr, 4) shouldEqual 123456L
      recSchema1.utf8StringPointer(recordAddr, 5).compare("Series 1".utf8(nativeMem)) shouldEqual 0
      RecordSchema.schemaID(recordAddr) shouldEqual schema1.schemaHash
    }

    it("should hash correctly with different ways of adding UTF8 fields") {
      // schema for part key with only a string
      val stringSchema = new RecordSchema(Seq(ColumnInfo("sc", ColumnType.StringColumn)), Some(0))
      val builder = new RecordBuilder(nativeMem)
      stringSchema.fixedStart shouldEqual 6

      val str = "Serie zero"
      val strBytes = str.getBytes()
      val utf8MedStr = str.utf8(nativeMem)

      builder.startNewRecord(stringSchema, 123)
      builder.addString(str)
      val addrStringAdd = builder.endRecord()

      builder.startNewRecord(stringSchema, 123)
      builder.addBlob(strBytes, UnsafeUtils.arayOffset, strBytes.size)
      val addrBlobAdd = builder.endRecord()

      builder.startNewRecord(stringSchema, 123)
      builder.addSlowly(strBytes)
      val addrAddSlowly = builder.endRecord()

      val addrAddReader = builder.addFromReader(SeqRowReader(Seq(str)), stringSchema, 123)

      // Should be able to extract and compare strings
      stringSchema.utf8StringPointer(addrStringAdd, 0).compare(utf8MedStr) shouldEqual 0
      stringSchema.utf8StringPointer(addrBlobAdd, 0).compare(utf8MedStr) shouldEqual 0
      stringSchema.utf8StringPointer(addrAddSlowly, 0).compare(utf8MedStr) shouldEqual 0
      stringSchema.utf8StringPointer(addrAddReader, 0).compare(utf8MedStr) shouldEqual 0

      // Hashes should all be equal and not initial hash
      stringSchema.partitionHash(addrStringAdd) should not equal (RecordBuilder.HASH_INIT)
      stringSchema.partitionHash(addrBlobAdd) should not equal (RecordBuilder.HASH_INIT)
      stringSchema.partitionHash(addrAddReader) should not equal (RecordBuilder.HASH_INIT)

      stringSchema.partitionHash(addrStringAdd) shouldEqual stringSchema.partitionHash(addrBlobAdd)
      stringSchema.partitionHash(addrStringAdd) shouldEqual stringSchema.partitionHash(addrAddReader)
      stringSchema.partitionHash(addrStringAdd) shouldEqual stringSchema.partitionHash(addrAddSlowly)

      // No schemaID should be added
      RecordSchema.schemaID(addrStringAdd) shouldEqual (123)
      RecordSchema.schemaID(addrBlobAdd) shouldEqual (123)
      RecordSchema.schemaID(addrAddReader) shouldEqual (123)
      RecordSchema.schemaID(addrAddSlowly) shouldEqual (123)
    }

    it("should add and get map fields with no predefined keys") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)
      val data = withMap(linearMultiSeries(), extraTags=extraTags).take(3)
      addToBuilder(builder, data, schema2)

      val containers = builder.allContainers
      containers should have length (1)
      // 56 (len + 5 long/double + 2 var + hash) + 10 + 2 + extraTagsLen + 10 * 2)
      containers.head.numBytes shouldEqual (12 + 3 * align4(68 + extraTagsLen + 2 + 18))

      containers.head.consumeRecords(consumer)
      records should have length (3)
      val (recordBase, recordOff) = records.head

      // in sorted order, the keys are: cloudProvider, instance, job, n, region
      lastIndex = -1
      recSchema2.consumeMapItems(recordBase, recordOff, 6, keyCheckConsumer)
      lastIndex shouldEqual 4   // 5 key-value pairs: 0, 1, 2, 3, 4

      recSchema2.consumeMapItems(recordBase, recordOff, 6, valuesCheckConsumer)
    }

    it("should add and get map fields with predefined keys") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)

      val data = withMap(linearMultiSeries(), extraTags=extraTags).take(3)
      addToBuilder(builder, data, dataset3.schema)

      val containers = builder.allContainers
      containers should have length (1)
      // predefined keys means use less bytes
      containers.head.numBytes should be < (3 * align4(70 + extraTagsLen + 2 + 20))

      containers.head.consumeRecords(consumer)
      records should have length (3)
      val (recordBase, recordOff) = records.head

      // in sorted order, the keys are: cloudProvider, instance, job, n, region
      lastIndex = -1
      schemaWithPredefKeys.consumeMapItems(recordBase, recordOff, 6, keyCheckConsumer)
      lastIndex shouldEqual 4   // 5 key-value pairs: 0, 1, 2, 3, 4

      schemaWithPredefKeys.consumeMapItems(recordBase, recordOff, 6, valuesCheckConsumer)
    }

    it("should add map fields with addMap() and populate unique hashes") {
      val labels = Map("job" -> "prometheus",
                       "dc" -> "AWS-USE", "instance" -> "0123892E342342A90",
                       "__name__" -> "host_cpu_load")

      val builder = new RecordBuilder(MemFactory.onHeapFactory)

      def addRec(n: Int): Long = {
        val pairs = (labels + ("n" -> n.toString)).map { case (k, v) => ZCUTF8(k) -> ZCUTF8(v) }.toMap

        builder.startNewRecord(schema2)
        val ts = System.currentTimeMillis
        builder.addLong(ts)     // timestamp
        builder.addDouble(1.0)  // min
        builder.addDouble(2.5)  // avg
        builder.addDouble(10.1) // max
        builder.addLong(123456L)  // count
        builder.addString(s"Series $n")   // series (partition key)
        builder.addMap(pairs)
        builder.endRecord()
      }

      val offset1 = addRec(1)
      val offset2 = addRec(2)
      val offset3 = addRec(3)
      val basebase = builder.allContainers.head.base

      val brHashes = new collection.mutable.HashSet[Int]
      var lastHash = -1
      Seq(offset1, offset2, offset3).foreach { off =>
        lastHash = recSchema2.partitionHash(basebase, off)
        brHashes += lastHash
        // println(s"XXX: offset = $off   hash = $lastHash")
      }

      brHashes.size shouldEqual 3

      // Adding the same content should result in the same hash
      val offset4 = addRec(3)
      recSchema2.partitionHash(basebase, offset4) shouldEqual lastHash
    }

    it("should copy records to new byte arrays and compare equally") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)
      val data = withMap(linearMultiSeries(), extraTags=extraTags).take(3)
      addToBuilder(builder, data, schema2)

      val containers = builder.allContainers
      containers should have length (1)
      containers.head.consumeRecords(consumer)
      records should have length (3)
      val (recordBase, recordOff) = records.head
      val bytes = recSchema2.asByteArray(recordBase, recordOff)
      bytes should not equal (recordBase)
      recSchema2.equals(recordBase, recordOff, bytes, UnsafeUtils.arayOffset) shouldEqual true
    }
  }

  // This method allows us to build a "partKey" schema BinaryRecord without using the RecordComparator method,
  // just to let us test partitionMatch() independently of buildPartKeyFromIngest()
  private def dataset2AddPartKeys(builder: RecordBuilder, data: Stream[Seq[Any]]) = {
    data.foreach { values =>
      builder.startNewRecord(dataset3.schema.partKeySchema, dataset3.schema.schemaHash)
      builder.addString(values(5).asInstanceOf[String])  // series (partition key)
      if (values.length > 6) {
        builder.startMap()
        values(6).asInstanceOf[Types.UTF8Map].toSeq.sortBy(_._1).foreach { case (k, v) =>
          builder.addMapKeyValue(k.bytes, v.bytes)
        }
        builder.endMap()
      }
      builder.endRecord()
    }
  }

  describe("RecordComparator") {
    val comparator2 = dataset3.schema.comparator
    val partSchema2 = comparator2.partitionKeySchema

    val ingestBuilder = new RecordBuilder(MemFactory.onHeapFactory)
    val ingestData = withMap(linearMultiSeries(), extraTags=extraTags).take(3)
    addToBuilder(ingestBuilder, ingestData, dataset3.schema)
    records.clear()
    ingestBuilder.allContainers.head.consumeRecords(consumer)
    val ingestRecords = records.toSeq.toBuffer   // make a copy

    it("should produce a valid partKeySchema") {
      partSchema2.partitionFieldStart shouldEqual Some(0)
      partSchema2.numFields shouldEqual 2
      partSchema2.predefinedKeys shouldEqual Seq("job", "instance")
      partSchema2.columnTypes shouldEqual schemaWithPredefKeys.columnTypes.takeRight(2)
    }

    it("partitionMatch() should return false for ingest/partKey records with different contents") {
      val partKeyBuilder = new RecordBuilder(MemFactory.onHeapFactory)

      // different sized var areas / length var fields
      val diffData1 = withMap(linearMultiSeries(), extraTags=tagsWithDiffLen) take 1
      dataset2AddPartKeys(partKeyBuilder, diffData1)
      partKeyBuilder.allContainers.head.consumeRecords(consumer)
      records should have length (1)
      val (partKeyBase, partKeyOff) = records.head
      comparator2.partitionMatch(ingestRecords.head._1, ingestRecords.head._2, partKeyBase, partKeyOff) shouldEqual false

      // same sized var areas, different contents
      val diffData2 = withMap(linearMultiSeries(), extraTags=tagsDiffSameLen) drop 1 take 2
      dataset2AddPartKeys(partKeyBuilder, diffData2)

      val partRecords = partKeyBuilder.allContainers.head.map { case (b, o) => (b, o) }
      partRecords.length shouldEqual 3
      comparator2.partitionMatch(ingestRecords(2)._1, ingestRecords(2)._2,
                                 partRecords(2)._1, partRecords(2)._2) shouldEqual false

      // identical var areas, diff primitive fields
    }

    it("partitionMatch() should return true for ingest/partKey records with identical contents") {
      val partKeyBuilder = new RecordBuilder(MemFactory.onHeapFactory)
      dataset2AddPartKeys(partKeyBuilder, ingestData)

      partKeyBuilder.allContainers.head.consumeRecords(consumer)
      records should have length (3)
      (0 to 2).foreach { n =>
        comparator2.partitionMatch(ingestRecords(n)._1, ingestRecords(n)._2, records(n)._1, records(n)._2) shouldEqual true
      }
    }

    it("should copy ingest BRs to partition key BRs correctly with buildPartKeyFromIngest()") {
      val partKeyBuilder = new RecordBuilder(MemFactory.onHeapFactory)

      ingestRecords.foreach { case (base, off) =>
        comparator2.buildPartKeyFromIngest(base, off, partKeyBuilder)
      }

      partKeyBuilder.allContainers.head.consumeRecords(consumer)
      records should have length (3)
      (0 to 2).foreach { n =>
        comparator2.partitionMatch(ingestRecords(n)._1, ingestRecords(n)._2, records(n)._1, records(n)._2) shouldEqual true
        schemaWithPredefKeys.partitionHash(ingestRecords(n)._1, ingestRecords(n)._2) shouldEqual (
          partSchema2.partitionHash(records(n)._1, records(n)._2))
      }

      // Should be able to read tags for each record
      val (recordBase, recordOff) = records.head

      // in sorted order, the keys are: cloudProvider, instance, job, n, region
      lastIndex = -1
      partSchema2.consumeMapItems(recordBase, recordOff, 1, keyCheckConsumer)
      lastIndex shouldEqual 4   // 5 key-value pairs: 0, 1, 2, 3, 4
    }

    it("should copy ingest BRs to partition key BRs correctly when data columns have a blob/histogram") {
      val ingestBuilder = new RecordBuilder(MemFactory.onHeapFactory)
      val data = linearHistSeries().take(3)
      data.foreach { row => ingestBuilder.addFromReader(SeqRowReader(row), histDataset.schema) }

      records.clear()
      ingestBuilder.allContainers.head.consumeRecords(consumer)
      val histRecords = records.toSeq.toBuffer   // make a copy

      // Now create partition keys
      val partKeyBuilder = new RecordBuilder(MemFactory.onHeapFactory)
      histRecords.foreach { case (base, off) =>
        histDataset.comparator.buildPartKeyFromIngest(base, off, partKeyBuilder)
      }

      records.clear()
      partKeyBuilder.allContainers.head.consumeRecords(consumer)
      records should have length (3)
      (0 to 2).foreach { n =>
        histDataset.comparator.partitionMatch(histRecords(n)._1, histRecords(n)._2,
                                              records(n)._1, records(n)._2) shouldEqual true
        histDataset.ingestionSchema.partitionHash(histRecords(n)._1, histRecords(n)._2) shouldEqual (
          histDataset.partKeySchema.partitionHash(records(n)._1, records(n)._2))
      }
    }
  }

  describe("hashing functions") {
    val labels = Map("job" -> "prometheus",
                     "dc" -> "AWS-USE", "instance" -> "0123892E342342A90",
                     "__name__" -> "host_cpu_load")
    import collection.JavaConverters._

    it("should sortAndComputeHashes") {
      val builder = new RecordBuilder(MemFactory.onHeapFactory)
      val pairs = new java.util.ArrayList(labels.toSeq.asJava)
      val hashes = RecordBuilder.sortAndComputeHashes(pairs)
      hashes.size shouldEqual labels.size
      pairs.asScala.map(_._1) shouldEqual Seq("__name__", "dc", "instance", "job")
      hashes.toSet.size shouldEqual 4
    }

    it("should compute shard key correctly") {
      val jobHash = BinaryRegion.hash32(labels("job").getBytes)
      val metricHash = BinaryRegion.hash32(labels("__name__").getBytes)

      RecordBuilder.shardKeyHash(Nil, labels("__name__")) shouldEqual (7*31 + metricHash)
      RecordBuilder.shardKeyHash(Seq(labels("job")), labels("__name__")) shouldEqual ((7*31 + jobHash)*31 + metricHash)
    }

    it("should combine hash excluding certain keys") {
      val pairs = new java.util.ArrayList(labels.toSeq.asJava)
      val hashes = RecordBuilder.sortAndComputeHashes(pairs)

      val hashAll = RecordBuilder.combineHashExcluding(pairs, hashes, Set.empty)

      // no such key, hash everything
      RecordBuilder.combineHashExcluding(pairs, hashes, Set("__name_")) shouldEqual hashAll
      RecordBuilder.combineHashExcluding(pairs, hashes, Set("__name__")) should not equal (hashAll)

      val pairs2 = new java.util.ArrayList((labels + ("le" -> "0.25")).toSeq.asJava)
      val hashes2 = RecordBuilder.sortAndComputeHashes(pairs2)

      RecordBuilder.combineHashExcluding(pairs2, hashes2, Set("le")) shouldEqual hashAll
    }
  }

  it("should trim metric name for _bucket _sum _count") {
    val options1 = schema1.options
    val metricName = RecordBuilder.trimShardColumn(options1, "__name__", "heap_usage_bucket")
    metricName shouldEqual "heap_usage"

    val metricName2 = RecordBuilder.trimShardColumn(options1, "__name__", "heap_usage_sum")
    metricName2 shouldEqual "heap_usage"

    val metricName3 = RecordBuilder.trimShardColumn(options1, "__name__", "heap_usage_count")
    metricName3 shouldEqual "heap_usage"

    val timeseriesDataset = Dataset.make("timeseries",
      Seq("tags:map"),
      Seq("timestamp:ts", "value:double"),
      Seq.empty,
      DatasetOptions(Seq("__name__", "job"), "__name__", false, Map("dummy" -> Seq("_bucket")))).get
    val metricName4 = RecordBuilder.trimShardColumn(timeseriesDataset.options, "__name__", "heap_usage_bucket")
    metricName4 shouldEqual "heap_usage_bucket"

    val metricName5 = RecordBuilder.trimShardColumn(options1, "__name__", "heap_usage_sum_count")
    metricName5 shouldEqual "heap_usage_sum"
  }

}
