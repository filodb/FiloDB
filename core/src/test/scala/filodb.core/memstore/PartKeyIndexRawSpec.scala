package filodb.core.memstore

import filodb.core.query.ColumnFilter
import filodb.core.query.Filter.{Equals, EqualsRegex, NotEquals, NotEqualsRegex}
import filodb.core.GdeltTestData.{dataset1, dataset6, dataset7, partKeyFromRecords, readers, records, uniqueReader}
import filodb.core.binaryrecord2.{RecordBuilder, RecordSchema}
import filodb.core.DatasetRef
import filodb.core.metadata.{PartitionSchema, Schemas}
import filodb.memory.format.UnsafeUtils.ZeroPointer
import filodb.memory.format.{UnsafeUtils, UTF8Wrapper}
import filodb.memory.format.ZeroCopyUTF8String.StringToUTF8
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.must.Matchers.{contain, not}
import org.scalatest.matchers.should.Matchers.{convertToAnyShouldWrapper, equal}

import java.io.{File, FileFilter}
import java.nio.file.{Files, StandardOpenOption}
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.DurationInt
import scala.util.Random

trait PartKeyIndexRawSpec {
  this: AnyFunSpec =>

  def partKeyOnHeap(partKeySchema: RecordSchema,
                    base: Any,
                    offset: Long): Array[Byte] = partKeySchema.asByteArray(base, offset)

  protected def createNewIndex(ref: DatasetRef,
                               schema: PartitionSchema,
                               facetEnabledAllLabels: Boolean,
                               facetEnabledShardKeyLabels: Boolean,
                               shardNum: Int,
                               retentionMillis: Long,
                               diskLocation: Option[File] = None,
                               lifecycleManager: Option[IndexMetadataStore] = None): PartKeyIndexRaw

  // scalastyle:off method.length
  // scalastyle:off cyclomatic.complexity
  def commonPartKeyTests(keyIndex: PartKeyIndexRaw, partBuilder: RecordBuilder): Unit = {
    it("should add part keys and parse filters correctly") {
      val start = System.currentTimeMillis()
      // Add the first ten keys and row numbers
      partKeyFromRecords(dataset6, records(dataset6, readers.take(10)), Some(partBuilder))
        .zipWithIndex.foreach { case (addr, i) =>
          keyIndex.addPartKey(partKeyOnHeap(dataset6.partKeySchema, ZeroPointer, addr), i, System.currentTimeMillis())()
        }
      val end = System.currentTimeMillis()
      keyIndex.refreshReadersBlocking()

      // Should get empty iterator when passing no filters
      val partNums1 = keyIndex.partIdsFromFilters(Nil, start, end)
      partNums1.toIterable() should contain theSameElementsAs List(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)

      // return only 4 partIds - empty filter
      val partNumsLimit = keyIndex.partIdsFromFilters(Nil, start, end, 4)
      partNumsLimit.length shouldEqual 4
      // It's not deterministic which 4 docs are returned, but we can check that a valid part ID is in the list
      List(0, 1, 2, 3, 4, 5, 6, 7, 8, 9) should contain allElementsOf partNumsLimit.toIterable()

      val filter2 = ColumnFilter("Actor2Code", Equals("GOV".utf8))
      val partNums2 = keyIndex.partIdsFromFilters(Seq(filter2), start, end)
      partNums2.toIterable() should contain theSameElementsAs List(7, 8, 9)

      // return only 2 partIds - with filter
      val partNumsLimitFilter = keyIndex.partIdsFromFilters(Seq(filter2), start, end, 2)
      partNumsLimitFilter.length shouldEqual  2
      List(7,8,9) should contain allElementsOf partNumsLimitFilter.toIterable()

      val filter3 = ColumnFilter("Actor2Name", Equals("REGIME".utf8))
      val partNums3 = keyIndex.partIdsFromFilters(Seq(filter3), start, end)
      partNums3.toIterable() should contain theSameElementsAs List(8, 9)

      val filter4 = ColumnFilter("Actor2Name", Equals("REGIME".utf8))
      val partNums4 = keyIndex.partIdsFromFilters(Seq(filter4), 10, start-1)
      partNums4 shouldEqual debox.Buffer.empty[Int]

      val filter5 = ColumnFilter("Actor2Name", Equals("REGIME".utf8))
      val partNums5 = keyIndex.partIdsFromFilters(Seq(filter5), end + 100, end + 100000)
      partNums5 should not equal debox.Buffer.empty[Int]

      val filter6 = ColumnFilter("Actor2Name", Equals("REGIME".utf8))
      val partNums6 = keyIndex.partIdsFromFilters(Seq(filter6), start - 10000, end )
      partNums6 should not equal debox.Buffer.empty[Int]

      val filter7 = ColumnFilter("Actor2Name", Equals("REGIME".utf8))
      val partNums7 = keyIndex.partIdsFromFilters(Seq(filter7), (start + end)/2, end + 1000 )
      partNums7 should not equal debox.Buffer.empty[Int]

      // tests to validate schema ID filter
      val filter8 = Seq( ColumnFilter("Actor2Code", Equals("GOV".utf8)),
        ColumnFilter("_type_", Equals("schemaID:46894".utf8)))
      val partNums8 = keyIndex.partIdsFromFilters(filter8, start, end)
      partNums8.toIterable() should contain theSameElementsAs List(7, 8, 9)

      val filter9 = Seq( ColumnFilter("Actor2Code", Equals("GOV".utf8)),
        ColumnFilter("_type_", Equals("prom-counter".utf8)))
      val partNums9 = keyIndex.partIdsFromFilters(filter9, start, end)
      partNums9.length shouldEqual 0
    }

    it("should parse filters with UTF8Wrapper and string correctly") {
      // Add the first ten keys and row numbers
      partKeyFromRecords(dataset6, records(dataset6, readers.take(10)), Some(partBuilder))
        .zipWithIndex.foreach { case (addr, i) =>
          keyIndex.addPartKey(partKeyOnHeap(dataset6.partKeySchema, ZeroPointer, addr), i, System.currentTimeMillis())()
        }

      keyIndex.refreshReadersBlocking()

      val filter2 = ColumnFilter("Actor2Name", Equals(UTF8Wrapper("REGIME".utf8)))
      val partNums2 = keyIndex.partIdsFromFilters(Seq(filter2), 0, Long.MaxValue)
      partNums2.toIterable() should contain theSameElementsAs List(8, 9)

      val filter3 = ColumnFilter("Actor2Name", Equals("REGIME"))
      val partNums3 = keyIndex.partIdsFromFilters(Seq(filter3), 0, Long.MaxValue)
      partNums3.toIterable() should contain theSameElementsAs List(8, 9)
    }

    it("should fetch part key records from filters correctly") {
      // Add the first ten keys and row numbers
      val pkrs = partKeyFromRecords(dataset6, records(dataset6, readers.take(10)), Some(partBuilder))
        .zipWithIndex.map { case (addr, i) =>
          val pk = partKeyOnHeap(dataset6.partKeySchema, ZeroPointer, addr)
          keyIndex.addPartKey(pk, i, i, i + 10)()
          PartKeyLuceneIndexRecord(pk, i, i + 10)
        }
      keyIndex.refreshReadersBlocking()

      val filter2 = ColumnFilter("Actor2Code", Equals("GOV".utf8))
      val result = keyIndex.partKeyRecordsFromFilters(Seq(filter2), 0, Long.MaxValue)
      val expected = Seq(pkrs(7), pkrs(8), pkrs(9))

      result.map(_.partKey.toSeq) should contain theSameElementsAs expected.map(_.partKey.toSeq)
      result.map( p => (p.startTime, p.endTime)) should contain theSameElementsAs expected.map( p => (p.startTime, p.endTime))
    }

    it("should fetch only two part key records from filters") {
      // Add the first ten keys and row numbers
      val pkrs = partKeyFromRecords(dataset6, records(dataset6, readers.take(10)), Some(partBuilder))
        .zipWithIndex.map { case (addr, i) =>
          val pk = partKeyOnHeap(dataset6.partKeySchema, ZeroPointer, addr)
          keyIndex.addPartKey(pk, i, i, i + 10)()
          PartKeyLuceneIndexRecord(pk, i, i + 10)
        }
      keyIndex.refreshReadersBlocking()

      val filter2 = ColumnFilter("Actor2Code", Equals("GOV".utf8))
      val result = keyIndex.partKeyRecordsFromFilters(Seq(filter2), 0, Long.MaxValue, 2)
      val expected = Seq(pkrs(7), pkrs(8), pkrs(9))

      result.length shouldEqual 2
      // Which two elements are chosen aren't deterministic - any of 7,8,9 may come back
      expected.map(_.partKey.toSeq) should contain allElementsOf result.map(_.partKey.toSeq)
      expected.map(p => (p.startTime, p.endTime)) should contain allElementsOf result.map(p => (p.startTime, p.endTime))
    }

    it("should fetch records from filters correctly with a missing label != with a non empty value") {
      // Weird case in prometheus where if a non existent label is used with != check with an empty value,
      // the returned result includes all results where the label is missing and those where the label exists
      // and the value is not equal to the provided value.
      // Check next test case for the check for empty value

      // Add the first ten keys and row numbers
      val pkrs = partKeyFromRecords(dataset6, records(dataset6, readers.take(10)), Some(partBuilder))
        .zipWithIndex.map { case (addr, i) =>
          val pk = partKeyOnHeap(dataset6.partKeySchema, ZeroPointer, addr)
          keyIndex.addPartKey(pk, i, i, i + 10)()
          PartKeyLuceneIndexRecord(pk, i, i + 10)
        }
      keyIndex.refreshReadersBlocking()

      // Filter != condition on a field that doesn't exist must not affect the result

      val filter1 = ColumnFilter("some", NotEquals("t".utf8))
      val filter2 = ColumnFilter("Actor2Code", Equals("GOV".utf8))
      val result = keyIndex.partKeyRecordsFromFilters(Seq(filter1, filter2), 0, Long.MaxValue)
      val expected = Seq(pkrs(7), pkrs(8), pkrs(9))

      result.map(_.partKey.toSeq) should contain theSameElementsAs expected.map(_.partKey.toSeq)
      result.map( p => (p.startTime, p.endTime)) should contain theSameElementsAs expected.map( p => (p.startTime, p.endTime))
    }

    it("should not fetch records from filters correctly with a missing label != with an empty value") {

      // Weird case in prometheus where if a non existent label is used with != check with an empty value,
      // the returned result is empty

      // Add the first ten keys and row numbers
      val pkrs = partKeyFromRecords(dataset6, records(dataset6, readers.take(10)), Some(partBuilder))
        .zipWithIndex.map { case (addr, i) =>
          val pk = partKeyOnHeap(dataset6.partKeySchema, ZeroPointer, addr)
          keyIndex.addPartKey(pk, i, i, i + 10)()
          PartKeyLuceneIndexRecord(pk, i, i + 10)
        }
      keyIndex.refreshReadersBlocking()

      // Filter != condition on a field that doesn't exist must not affect the result

      val filter1 = ColumnFilter("some", NotEquals("".utf8))
      val filter2 = ColumnFilter("Actor2Code", Equals("GOV".utf8))
      val result = keyIndex.partKeyRecordsFromFilters(Seq(filter1, filter2), 0, Long.MaxValue)
      result.isEmpty shouldBe true
    }

    it("should add part keys and fetch startTimes correctly for more than 1024 keys") {
      val numPartIds = 3000 // needs to be more than 1024 to test the lucene term limit
      val start = System.currentTimeMillis()
      // we dont care much about the partKey here, but the startTime against partId.
      val partKeys = Stream.continually(readers.head).take(numPartIds).toList
      partKeyFromRecords(dataset6, records(dataset6, partKeys), Some(partBuilder))
        .zipWithIndex.foreach { case (addr, i) =>
          keyIndex.addPartKey(partKeyOnHeap(dataset6.partKeySchema, ZeroPointer, addr), i, start + i)()
        }
      keyIndex.refreshReadersBlocking()

      val startTimes = keyIndex.startTimeFromPartIds((0 until numPartIds).iterator)
      for { i <- 0 until numPartIds} {
        startTimes(i) shouldEqual start + i
      }
    }

    it("should upsert part keys with endtime by partKey should work and return only active partkeys") {
      // Add the first ten keys and row numbers
      val expectedSHA256PartIds = ArrayBuffer.empty[String]
      partKeyFromRecords(dataset6, records(dataset6, uniqueReader.take(10)), Some(partBuilder))
        .zipWithIndex.foreach { case (addr, i) =>
          val time = System.currentTimeMillis()
          val pkOnHeap = partKeyOnHeap(dataset6.partKeySchema, ZeroPointer, addr)
          keyIndex.addPartKey(pkOnHeap, -1, time)(
            pkOnHeap.length,
            PartKeyLuceneIndex.partKeyByteRefToSHA256Digest(pkOnHeap, 0, pkOnHeap.length))
          if (i % 2 == 0) {
            keyIndex.upsertPartKey(pkOnHeap, -1, time, time + 300)(pkOnHeap.length,
              PartKeyLuceneIndex.partKeyByteRefToSHA256Digest(pkOnHeap, 0, pkOnHeap.length))
          } else {
            expectedSHA256PartIds.append(PartKeyLuceneIndex.partKeyByteRefToSHA256Digest(pkOnHeap, 0, pkOnHeap.length))
          }
        }
      keyIndex.refreshReadersBlocking()

      keyIndex.indexNumEntries shouldEqual 10

      val activelyIngestingParts =
        keyIndex.partKeyRecordsFromFilters(Nil, System.currentTimeMillis() + 500, Long.MaxValue)
      activelyIngestingParts.size shouldBe 5


      val actualSha256PartIds = activelyIngestingParts.map(r => {
        PartKeyLuceneIndex.partKeyByteRefToSHA256Digest(r.partKey, 0, r.partKey.length)
      })

      expectedSHA256PartIds.toSet shouldEqual actualSha256PartIds.toSet
    }

    it("should add part keys and fetch partIdsEndedBefore and removePartKeys correctly for more than 1024 keys") {
      val numPartIds = 3000 // needs to be more than 1024 to test the lucene term limit
      val start = 1000
      // we dont care much about the partKey here, but the startTime against partId.
      val partKeys = Stream.continually(readers.head).take(numPartIds).toList
      partKeyFromRecords(dataset6, records(dataset6, partKeys), Some(partBuilder))
        .zipWithIndex.foreach { case (addr, i) =>
          keyIndex.addPartKey(partKeyOnHeap(dataset6.partKeySchema, ZeroPointer, addr), i, start + i, start + i + 100)()
        }
      keyIndex.refreshReadersBlocking()

      val pIds = keyIndex.partIdsEndedBefore(start + 200)
      val pIdsList = pIds.toList()
      for { i <- 0 until numPartIds} {
        pIdsList.contains(i) shouldEqual (if (i <= 100) true else false)
      }

      keyIndex.removePartKeys(pIds)
      keyIndex.refreshReadersBlocking()

      for { i <- 0 until numPartIds} {
        keyIndex.partKeyFromPartId(i).isDefined shouldEqual (if (i <= 100) false else true)
      }

    }

    it("should add part keys and removePartKeys (without partIds) correctly for more than 1024 keys with del count") {
      // Identical to test
      // it("should add part keys and fetch partIdsEndedBefore and removePartKeys correctly for more than 1024 keys")
      // However, combines them not requiring us to get partIds and pass them tpo remove
      val numPartIds = 3000 // needs to be more than 1024 to test the lucene term limit
      val start = 1000
      // we dont care much about the partKey here, but the startTime against partId.
      val partKeys = Stream.continually(readers.head).take(numPartIds).toList
      partKeyFromRecords(dataset6, records(dataset6, partKeys), Some(partBuilder))
        .zipWithIndex.foreach { case (addr, i) =>
          keyIndex.addPartKey(partKeyOnHeap(dataset6.partKeySchema, ZeroPointer, addr), i, start + i, start + i + 100)()
        }
      keyIndex.refreshReadersBlocking()
      val pIdsList = keyIndex.partIdsEndedBefore(start + 200).toList()
      for { i <- 0 until numPartIds} {
        pIdsList.contains(i) shouldEqual (i <= 100)
      }
      val numDeleted = keyIndex.removePartitionsEndedBefore(start + 200)
      numDeleted shouldEqual pIdsList.size
      keyIndex.refreshReadersBlocking()

      // Ensure everything expected is present or deleted
      for { i <- 0 until numPartIds} {
        // Everything with partId > 100 should be present
        keyIndex.partKeyFromPartId(i).isDefined shouldEqual (i > 100)
      }

      // Important, while the unit test uses partIds to assert the presence or absence before and after deletion
      // it is no longer required to have partIds in the index on non unit test setup
    }

    it("should add part keys and removePartKeys (without partIds) correctly for more than 1024 keys w/o del count") {
      // identical
      // to should add part keys and removePartKeys (without partIds) correctly for more than 1024 keys w/o del count
      // except that this one returns 0 for numDocuments deleted

      val numPartIds = 3000 // needs to be more than 1024 to test the lucene term limit
      val start = 1000
      val partKeys = Stream.continually(readers.head).take(numPartIds).toList
      partKeyFromRecords(dataset6, records(dataset6, partKeys), Some(partBuilder))
        .zipWithIndex.foreach { case (addr, i) =>
          keyIndex.addPartKey(partKeyOnHeap(dataset6.partKeySchema, ZeroPointer, addr), i, start + i, start + i + 100)()
        }
      keyIndex.refreshReadersBlocking()
      val pIdsList = keyIndex.partIdsEndedBefore(start + 200).toList()
      for { i <- 0 until numPartIds} {
        pIdsList.contains(i) shouldEqual (i <= 100)
      }
      val numDeleted = keyIndex.removePartitionsEndedBefore(start + 200, false)
      numDeleted shouldEqual 0
      keyIndex.refreshReadersBlocking()

      // Ensure everything expected is present or deleted
      for { i <- 0 until numPartIds} {
        // Everything with partId > 100 should be present
        keyIndex.partKeyFromPartId(i).isDefined shouldEqual (i > 100)
      }

      // Important, while the unit test uses partIds to assert the presence or absence before and after deletion
      // it is no longer required to have partIds in the index on non unit test setup
    }

    it("should update part keys with endtime and parse filters correctly") {
      val start = System.currentTimeMillis()
      // Add the first ten keys and row numbers
      partKeyFromRecords(dataset6, records(dataset6, readers.take(10)), Some(partBuilder))
        .zipWithIndex.foreach { case (addr, i) =>
          val time = System.currentTimeMillis()
          keyIndex.addPartKey(partKeyOnHeap(dataset6.partKeySchema, ZeroPointer, addr), i, time)()
          keyIndex.refreshReadersBlocking() // updates need to be able to read startTime from index, so commit
          keyIndex.updatePartKeyWithEndTime(partKeyOnHeap(dataset6.partKeySchema, ZeroPointer, addr), i, time + 10000)()
        }
      val end = System.currentTimeMillis()
      keyIndex.refreshReadersBlocking()

      // Should get empty iterator when passing no filters
      val partNums1 = keyIndex.partIdsFromFilters(Nil, start, end)
      partNums1.toIterable() should contain theSameElementsAs List(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)

      val filter2 = ColumnFilter("Actor2Code", Equals("GOV".utf8))
      val partNums2 = keyIndex.partIdsFromFilters(Seq(filter2), start, end)
      partNums2.toIterable() should contain theSameElementsAs  List(7, 8, 9)

      val filter3 = ColumnFilter("Actor2Name", Equals("REGIME".utf8))
      val partNums3 = keyIndex.partIdsFromFilters(Seq(filter3), start, end)
      partNums3.toIterable() should contain theSameElementsAs List(8, 9)

      val filter4 = ColumnFilter("Actor2Name", Equals("REGIME".utf8))
      val partNums4 = keyIndex.partIdsFromFilters(Seq(filter4), 10, start-1)
      partNums4 shouldEqual debox.Buffer.empty[Int]

      val filter5 = ColumnFilter("Actor2Name", Equals("REGIME".utf8))
      val partNums5 = keyIndex.partIdsFromFilters(Seq(filter5), end + 20000, end + 100000)
      partNums5 shouldEqual debox.Buffer.empty[Int]

      val filter6 = ColumnFilter("Actor2Name", Equals("REGIME".utf8))
      val partNums6 = keyIndex.partIdsFromFilters(Seq(filter6), start - 10000, end-1 )
      partNums6 should not equal debox.Buffer.empty[Int]

      val filter7 = ColumnFilter("Actor2Name", Equals("REGIME".utf8))
      val partNums7 = keyIndex.partIdsFromFilters(Seq(filter7), (start + end)/2, end + 1000 )
      partNums7 should not equal debox.Buffer.empty[Int]
    }

    it("should obtain indexed names and values") {
      // Add the first ten keys and row numbers
      partKeyFromRecords(dataset6, records(dataset6, readers.take(10)), Some(partBuilder))
        .zipWithIndex.foreach { case (addr, i) =>
          keyIndex.addPartKey(partKeyOnHeap(dataset6.partKeySchema, ZeroPointer, addr), i, System.currentTimeMillis())()
        }

      keyIndex.refreshReadersBlocking()

      keyIndex.indexNames(10).toList should contain theSameElementsAs Seq("_type_", "Actor2Code", "Actor2Name")
      keyIndex.indexValues("not_found").toSeq should equal (Nil)

      val infos = Seq("AFR", "CHN", "COP", "CVL", "EGYEDU").map(_.utf8).map(TermInfo(_, 1))
      val top2infos = Seq(TermInfo("GOV".utf8, 3), TermInfo("AGR".utf8, 2))
      // top 2 items by frequency
      keyIndex.indexValues("Actor2Code", 2) shouldEqual top2infos
      val allValues = keyIndex.indexValues("Actor2Code")
      allValues take 2 shouldEqual top2infos
      allValues.drop(2).toSet shouldEqual infos.toSet
    }

    it("should be able to AND multiple filters together") {
      // Add the first ten keys and row numbers
      partKeyFromRecords(dataset6, records(dataset6, readers.take(10)), Some(partBuilder))
        .zipWithIndex.foreach { case (addr, i) =>
          keyIndex.addPartKey(partKeyOnHeap(dataset6.partKeySchema, ZeroPointer, addr), i, System.currentTimeMillis())()
        }

      keyIndex.refreshReadersBlocking()

      val filters1 = Seq(ColumnFilter("Actor2Code", Equals("GOV".utf8)),
        ColumnFilter("Actor2Name", Equals("REGIME".utf8)))
      val partNums1 = keyIndex.partIdsFromFilters(filters1, 0, Long.MaxValue)
      partNums1.toIterable() should contain theSameElementsAs List(8, 9)

      val filters2 = Seq(ColumnFilter("Actor2Code", Equals("GOV".utf8)),
        ColumnFilter("Actor2Name", Equals("CHINA".utf8)))
      val partNums2 = keyIndex.partIdsFromFilters(filters2, 0, Long.MaxValue)
      partNums2 shouldEqual debox.Buffer.empty[Int]
    }

    it("should be able to convert pipe regex to TermInSetQuery") {
      // Add the first ten keys and row numbers
      partKeyFromRecords(dataset6, records(dataset6, readers.take(99)), Some(partBuilder))
        .zipWithIndex.foreach { case (addr, i) =>
          keyIndex.addPartKey(partKeyOnHeap(dataset6.partKeySchema, ZeroPointer, addr), i, System.currentTimeMillis())()
        }
      keyIndex.refreshReadersBlocking()

      val filters1 = Seq(ColumnFilter("Actor2Code", EqualsRegex("GOV|KHM|LAB|MED".utf8)))
      val partNums1 = keyIndex.partIdsFromFilters(filters1, 0, Long.MaxValue)
      partNums1.toIterable() should contain theSameElementsAs List(7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 22, 23, 24, 25, 26, 28, 29, 73, 81, 90)
    }

    it("should be able to convert prefix regex to PrefixQuery") {
      // Add the first ten keys and row numbers
      partKeyFromRecords(dataset6, records(dataset6, readers.take(99)), Some(partBuilder))
        .zipWithIndex.foreach { case (addr, i) =>
          keyIndex.addPartKey(partKeyOnHeap(dataset6.partKeySchema, ZeroPointer, addr), i, System.currentTimeMillis())()
        }
      keyIndex.refreshReadersBlocking()

      val filters1 = Seq(ColumnFilter("Actor2Name", EqualsRegex("C.*".utf8)))
      val partNums1 = keyIndex.partIdsFromFilters(filters1, 0, Long.MaxValue)
      partNums1.toIterable() should contain theSameElementsAs List(3, 12, 22, 23, 24, 31, 59, 60, 66, 69, 72, 78, 79, 80, 88, 89)
    }


    it("should ignore unsupported columns and return empty filter") {
      val index2 = createNewIndex(dataset1.ref, dataset1.schema.partition, true, true, 0, 1.hour.toMillis)
      partKeyFromRecords(dataset1, records(dataset1, readers.take(10))).zipWithIndex.foreach { case (addr, i) =>
        index2.addPartKey(partKeyOnHeap(dataset6.partKeySchema, ZeroPointer, addr), i, System.currentTimeMillis())()
      }
      keyIndex.refreshReadersBlocking()

      val filters1 = Seq(ColumnFilter("Actor2Code", Equals("GOV".utf8)), ColumnFilter("Year", Equals(1979)))
      val partNums1 = index2.partIdsFromFilters(filters1, 0, Long.MaxValue)
      partNums1 shouldEqual debox.Buffer.empty[Int]
    }

    it("should be able to fetch partKey from partId and partId from partKey") {
      // Add the first ten keys and row numbers
      partKeyFromRecords(dataset6, records(dataset6, readers.take(10)), Some(partBuilder))
        .zipWithIndex.foreach { case (addr, i) =>
          val partKeyBytes = partKeyOnHeap(dataset6.partKeySchema, ZeroPointer, addr)
          keyIndex.addPartKey(partKeyBytes, i, System.currentTimeMillis())()
          keyIndex.refreshReadersBlocking()
          keyIndex.partKeyFromPartId(i).get.bytes shouldEqual partKeyBytes
          //      keyIndex.partIdFromPartKey(new BytesRef(partKeyBytes)) shouldEqual i
        }
    }

    it("should be able to fetch label names efficiently using facets") {

      val index3 = createNewIndex(DatasetRef("prometheus"), Schemas.promCounter.partition,
        true, true, 0, 1.hour.toMillis)
      val seriesTags = Map("_ws_".utf8 -> "my_ws".utf8,
        "_ns_".utf8 -> "my_ns".utf8)

      // create 1000 time series with 10 metric names
      for { i <- 0 until 1000} {
        val dynamicLabelNum = i % 5
        val infoLabelNum = i % 10
        val partKey = partBuilder.partKeyFromObjects(Schemas.promCounter, s"counter",
          seriesTags + (s"dynamicLabel$dynamicLabelNum".utf8 -> s"dynamicLabelValue".utf8)
            + (s"infoLabel$infoLabelNum".utf8 -> s"infoLabelValue".utf8)
        )
        index3.addPartKey(partKeyOnHeap(Schemas.promCounter.partition.binSchema, ZeroPointer, partKey), i, 5)()
      }
      index3.refreshReadersBlocking()

      val filters1 = Seq(ColumnFilter("_ws_", Equals("my_ws")), ColumnFilter("_metric_", Equals("counter")))

      val partNums1 = index3.partIdsFromFilters(filters1, 0, Long.MaxValue)
      partNums1.length shouldEqual 1000

      val labelValues1 = index3.labelNamesEfficient(filters1, 0, Long.MaxValue)
      labelValues1.toSet shouldEqual (0 until 5).map(c => s"dynamicLabel$c").toSet ++
        (0 until 10).map(c => s"infoLabel$c").toSet ++
        Set("_ns_", "_ws_", "_metric_", "_type_")
    }

    it("should be able to fetch label values efficiently using facets") {
      val index3 = createNewIndex(DatasetRef("prometheus"), Schemas.promCounter.partition,
        true, true, 0, 1.hour.toMillis)
      val seriesTags = Map("_ws_".utf8 -> "my_ws".utf8,
        "_ns_".utf8 -> "my_ns".utf8)

      // create 1000 time series with 10 metric names
      for { i <- 0 until 1000} {
        val counterNum = i % 10
        val partKey = partBuilder.partKeyFromObjects(Schemas.promCounter, s"counter$counterNum",
          seriesTags + ("instance".utf8 -> s"instance$i".utf8))
        index3.addPartKey(partKeyOnHeap(Schemas.promCounter.partition.binSchema, ZeroPointer, partKey), i, 5)()
      }
      index3.refreshReadersBlocking()
      val filters1 = Seq(ColumnFilter("_ws_", Equals("my_ws")))

      val partNums1 = index3.partIdsFromFilters(filters1, 0, Long.MaxValue)
      partNums1.length shouldEqual 1000

      val labelValues1 = index3.labelValuesEfficient(filters1, 0, Long.MaxValue, "_metric_")
      labelValues1.toSet shouldEqual (0 until 10).map(c => s"counter$c").toSet

      val filters2 = Seq(ColumnFilter("_ws_", Equals("NotExist")))
      val labelValues2 = index3.labelValuesEfficient(filters2, 0, Long.MaxValue, "_metric_")
      labelValues2.size shouldEqual 0

      val filters3 = Seq(ColumnFilter("_metric_", Equals("counter1")))
      val labelValues3 = index3.labelValuesEfficient(filters3, 0, Long.MaxValue, "_metric_")
      labelValues3 shouldEqual Seq("counter1")

      val labelValues4 = index3.labelValuesEfficient(filters1, 0, Long.MaxValue, "instance", 1000)
      labelValues4.toSet shouldEqual (0 until 1000).map(c => s"instance$c").toSet
    }

    // Testcases to test additionalFacet config

    it("should be able to fetch label values efficiently using additonal facets") {
      val facetIndex = createNewIndex(dataset7.ref, dataset7.schema.partition,
        true, true, 0, 1.hour.toMillis)
      val addedKeys = partKeyFromRecords(dataset7, records(dataset7, readers.take(10)), Some(partBuilder))
        .zipWithIndex.map { case (addr, i) =>
          val start = Math.abs(Random.nextLong())
          facetIndex.addPartKey(partKeyOnHeap(dataset7.partKeySchema, ZeroPointer, addr), i, start)()
        }
      facetIndex.refreshReadersBlocking()
      val filters1 = Seq.empty

      val partNums1 = facetIndex.partIdsFromFilters(filters1, 0, Long.MaxValue)
      partNums1.length shouldEqual 10

      val labelValues1 = facetIndex.labelValuesEfficient(filters1, 0, Long.MaxValue, "Actor2Code")
      labelValues1.length shouldEqual 7

      val labelValues2 = facetIndex.labelValuesEfficient(filters1, 0, Long.MaxValue, "Actor2Code-Actor2Name")
      labelValues2.length shouldEqual 8

      val labelValues3 = facetIndex.labelValuesEfficient(filters1, 0, Long.MaxValue, "Actor2Name-Actor2Code")
      labelValues3.length shouldEqual 8

      labelValues1.sorted.toSet shouldEqual labelValues2.map(_.split("\u03C0")(0)).sorted.toSet
      labelValues1.sorted.toSet shouldEqual labelValues3.map(_.split("\u03C0")(1)).sorted.toSet

      val filters2 = Seq(ColumnFilter("Actor2Code", Equals("GOV")))

      val labelValues12 = facetIndex.labelValuesEfficient(filters2, 0, Long.MaxValue, "Actor2Name")
      labelValues12.length shouldEqual 2

      val labelValues22 = facetIndex.labelValuesEfficient(filters2, 0, Long.MaxValue, "Actor2Code-Actor2Name")
      labelValues22.length shouldEqual 2

      val labelValues32 = facetIndex.labelValuesEfficient(filters2, 0, Long.MaxValue, "Actor2Name-Actor2Code")
      labelValues32.length shouldEqual 2

      labelValues12.sorted shouldEqual labelValues22.map(_.split("\u03C0")(1)).sorted
      labelValues12.sorted shouldEqual labelValues32.map(_.split("\u03C0")(0)).sorted

    }


    it("must clean the input directory for Index state apart from Synced and Refreshing") {
      val events = ArrayBuffer.empty[(IndexState.Value, Long)]
      IndexState.values.foreach {
        indexState =>
          val indexDirectory = new File(
            System.getProperty("java.io.tmpdir"), "part-key-lucene-index-event")
          val shardDirectory = new File(indexDirectory, dataset6.ref + File.separator + "0")
          shardDirectory.mkdirs()
          new File(shardDirectory, "empty").createNewFile()
          // Validate the file named empty exists
          assert(shardDirectory.list().exists(_.equals("empty")))
          val index = createNewIndex(dataset6.ref, dataset6.schema.partition, true, true,0, 1.hour.toMillis,
            Some(indexDirectory),
            Some(new IndexMetadataStore {
              def currentState(datasetRef: DatasetRef, shard: Int): (IndexState.Value, Option[Long]) =
                (indexState, None)

              def updateState(datasetRef: DatasetRef, shard: Int, state: IndexState.Value, time: Long): Unit = {
                events.append((state, time))
              }

              override def initState(datasetRef: DatasetRef, shard: Int): (IndexState.Value, Option[Long]) =
                currentState(datasetRef: DatasetRef, shard: Int)

              override def updateInitState(datasetRef: DatasetRef, shard: Int, state: IndexState.Value, time: Long): Unit
              = {

              }
            }))
          index.closeIndex()
          events.toList match {
            case (IndexState.Empty, _) :: Nil if indexState != IndexState.Synced && indexState != IndexState.Refreshing =>
              // The file originally present must not be available
              assert(!shardDirectory.list().exists(_.equals("empty")))
              events.clear()
            case Nil  if indexState == IndexState.Synced
              || indexState == IndexState.Refreshing
              || indexState == IndexState.Empty     =>
              // Empty state denotes the FS is empty, it is not cleaned up again to ensure its empty
              // The file originally present "must" be available, which means no cleanup was done
              assert(shardDirectory.list().exists(_.equals("empty")))
            case _                                                                                                      =>
              fail("Expected an index state Empty after directory cleanup")
          }
      }
    }

    it("Should update the state as empty after the cleanup is from a corrupt index") {
      val events = ArrayBuffer.empty[(IndexState.Value, Long)]
      IndexState.values.foreach {
        indexState =>
          val indexDirectory = new File(
            System.getProperty("java.io.tmpdir"), "part-key-lucene-index-event")
          val shardDirectory = new File(indexDirectory, dataset6.ref + File.separator + "0")
          // Delete directory to create an index from scratch
          scala.reflect.io.Directory(shardDirectory).deleteRecursively()
          shardDirectory.mkdirs()
          // Validate the file named empty exists
          val index = createNewIndex(dataset6.ref, dataset6.schema.partition, true, true,0, 1.hour.toMillis,
            Some(indexDirectory),None)
          // Add some index entries
          val seriesTags = Map("_ws_".utf8 -> "my_ws".utf8,
            "_ns_".utf8 -> "my_ns".utf8)
          for { i <- 0 until 1000} {
            val counterNum = i % 10
            val partKey = partBuilder.partKeyFromObjects(Schemas.promCounter, s"counter$counterNum",
              seriesTags + ("instance".utf8 -> s"instance$i".utf8))
            index.addPartKey(partKeyOnHeap(Schemas.promCounter.partition.binSchema, ZeroPointer, partKey), i, 5)()
          }

          index.closeIndex()
          // Garble some index files to force a index corruption
          // Just add some junk to the end of the segment files

          shardDirectory.listFiles(new FileFilter {
            override def accept(pathname: File): Boolean = {
              pathname.toString.contains("segment") || pathname.toString.contains(".term")
            }
          }).foreach( file => {
            Files.writeString(file.toPath, "Hello", StandardOpenOption.APPEND)
          })

          createNewIndex(dataset6.ref, dataset6.schema.partition, true, true,0, 1.hour.toMillis,
            Some(indexDirectory),
            Some( new IndexMetadataStore {
              def currentState(datasetRef: DatasetRef, shard: Int): (IndexState.Value, Option[Long]) =
                (indexState, None)

              def updateState(datasetRef: DatasetRef, shard: Int, state: IndexState.Value, time: Long): Unit = {
                assert(shardDirectory.list().length == 0)
                events.append((state, time))
              }

              override def initState(datasetRef: DatasetRef, shard: Int): (IndexState.Value, Option[Long]) =
                currentState(datasetRef: DatasetRef, shard: Int)

              override def updateInitState(datasetRef: DatasetRef, shard: Int, state: IndexState.Value, time: Long): Unit
              = {

              }

            })).closeIndex()
          // For all states, including states where Index is Synced because the index is corrupt,
          // the shard directory should be cleared and the new state should be Empty
          events.toList match {
            case (IndexState.Empty, _) :: Nil =>
              // The file originally present must not be available
              assert(!shardDirectory.list().exists(_.equals("empty")))
              events.clear()
            case other                                                               =>
              fail(s"Expected an index state Empty after directory cleanup, got ${other}")
          }
      }
    }

    it("should get a single match for part keys by a filter") {

      val pkrs = partKeyFromRecords(dataset6, records(dataset6, readers.take(10)), Some(partBuilder))
        .zipWithIndex.map { case (addr, i) =>
          val pk = partKeyOnHeap(dataset6.partKeySchema, ZeroPointer, addr)
          keyIndex.addPartKey(pk, -1, i, i + 10)(
            pk.length, PartKeyLuceneIndex.partKeyByteRefToSHA256Digest(pk, 0, pk.length))
          PartKeyLuceneIndexRecord(pk, i, i + 10)
        }
      keyIndex.refreshReadersBlocking()

      val filter1 = ColumnFilter("Actor2Code", Equals("GOV".utf8))
      val partKeyOpt = keyIndex.singlePartKeyFromFilters(Seq(filter1), 4, 10)

      partKeyOpt.isDefined shouldBe true
      partKeyOpt.get shouldEqual pkrs(7).partKey

      val filter2 = ColumnFilter("Actor2Code", Equals("NonExist".utf8))
      keyIndex.singlePartKeyFromFilters(Seq(filter2), 4, 10) shouldBe None
    }

    it("should get a single match for part keys through a field with empty value") {
      val pkrs = partKeyFromRecords(dataset6, records(dataset6, readers.slice(95, 96)), Some(partBuilder))
        .zipWithIndex.map { case (addr, i) =>
          val pk = partKeyOnHeap(dataset6.partKeySchema, ZeroPointer, addr)
          keyIndex.addPartKey(pk, -1, i, i + 10)(
            pk.length, PartKeyLuceneIndex.partKeyByteRefToSHA256Digest(pk, 0, pk.length))
          PartKeyLuceneIndexRecord(pk, i, i + 10)
        }
      keyIndex.refreshReadersBlocking()

      val filter1_found = ColumnFilter("Actor2Code", Equals(""))
      val partKeyOpt = keyIndex.singlePartKeyFromFilters(Seq(filter1_found), 4, 10)
      partKeyOpt.isDefined shouldBe true
      partKeyOpt.get shouldEqual pkrs.head.partKey

      val filter2_found = ColumnFilter("Actor2Code", EqualsRegex(""))
      val partKeyOpt2 = keyIndex.singlePartKeyFromFilters(Seq(filter2_found), 4, 10)
      partKeyOpt2.isDefined shouldBe true
      partKeyOpt2.get shouldEqual pkrs.head.partKey

      val filter3_found = ColumnFilter("Actor2Code", EqualsRegex("^$"))
      val partKeyOpt3 = keyIndex.singlePartKeyFromFilters(Seq(filter3_found), 4, 10)
      partKeyOpt3.isDefined shouldBe true
      partKeyOpt3.get shouldEqual pkrs.head.partKey
    }

    it("should get a single match for part keys through a non-existing field") {
      val pkrs = partKeyFromRecords(dataset6, records(dataset6, readers.take(10)), Some(partBuilder))
        .zipWithIndex.map { case (addr, i) =>
          val pk = partKeyOnHeap(dataset6.partKeySchema, ZeroPointer, addr)
          keyIndex.addPartKey(pk, -1, i, i + 10)(
            pk.length, PartKeyLuceneIndex.partKeyByteRefToSHA256Digest(pk, 0, pk.length))
          PartKeyLuceneIndexRecord(pk, i, i + 10)
        }
      keyIndex.refreshReadersBlocking()

      val filter1_found = ColumnFilter("NonExistingField", Equals(""))
      val partKeyOpt = keyIndex.singlePartKeyFromFilters(Seq(filter1_found), 4, 10)
      partKeyOpt.isDefined shouldBe true
      partKeyOpt.get shouldEqual pkrs.head.partKey

      val filter2_found = ColumnFilter("NonExistingField", EqualsRegex(""))
      val partKeyOpt2 = keyIndex.singlePartKeyFromFilters(Seq(filter2_found), 4, 10)
      partKeyOpt2.isDefined shouldBe true
      partKeyOpt2.get shouldEqual pkrs.head.partKey

      val filter3_found = ColumnFilter("NonExistingField", EqualsRegex("^$"))
      val partKeyOpt3 = keyIndex.singlePartKeyFromFilters(Seq(filter3_found), 4, 10)
      partKeyOpt3.isDefined shouldBe true
      partKeyOpt3.get shouldEqual pkrs.head.partKey
    }

    it("should get a single match for part keys by a regex filter") {
      val pkrs = partKeyFromRecords(dataset6, records(dataset6, readers.take(10)), Some(partBuilder))
        .zipWithIndex.map { case (addr, i) =>
          val pk = partKeyOnHeap(dataset6.partKeySchema, ZeroPointer, addr)
          keyIndex.addPartKey(pk, -1, i, i + 10)(
            pk.length, PartKeyLuceneIndex.partKeyByteRefToSHA256Digest(pk, 0, pk.length))
          PartKeyLuceneIndexRecord(pk, i, i + 10)
        }
      keyIndex.refreshReadersBlocking()

      val filter1_found = ColumnFilter("Actor2Code", EqualsRegex("""^GO.*$"""))
      val partKeyOpt = keyIndex.singlePartKeyFromFilters(Seq(filter1_found), 4, 10)
      partKeyOpt.isDefined shouldBe true
      partKeyOpt.get shouldEqual pkrs(7).partKey

      val filter1_not_found = ColumnFilter("Actor2Code", EqualsRegex("""^GO.*\$"""))
      keyIndex.singlePartKeyFromFilters(Seq(filter1_not_found), 4, 10) shouldBe None

      val filter2 = ColumnFilter("Actor2Code", NotEqualsRegex("^.*".utf8))
      keyIndex.singlePartKeyFromFilters(Seq(filter2), 4, 10) shouldBe None
    }

    it("Should update the state as TriggerRebuild and throw an exception for any error other than CorruptIndexException")
    {
      val events = ArrayBuffer.empty[(IndexState.Value, Long)]
      IndexState.values.foreach {
        indexState =>
          val indexDirectory = new File(
            System.getProperty("java.io.tmpdir"), "part-key-lucene-index-event")
          val shardDirectory = new File(indexDirectory, dataset6.ref + File.separator + "0")
          shardDirectory.mkdirs()
          new File(shardDirectory, "empty").createNewFile()
          Files.writeString(new File(shardDirectory, "meta.json").toPath, "Hello", StandardOpenOption.CREATE)
          // Make directory readonly to for an IOException when attempting to write
          shardDirectory.setWritable(false)
          // Validate the file named empty exists
          assert(shardDirectory.list().exists(_.equals("empty")))
          try {
            val index = createNewIndex(dataset6.ref, dataset6.schema.partition, true, true,0, 1.hour.toMillis,
              Some(indexDirectory),
              Some( new IndexMetadataStore {
                def currentState(datasetRef: DatasetRef, shard: Int): (IndexState.Value, Option[Long]) =
                  (indexState, None)

                def updateState(datasetRef: DatasetRef, shard: Int, state: IndexState.Value, time: Long): Unit = {
                  events.append((state, time))
                }

                override def initState(datasetRef: DatasetRef, shard: Int): (IndexState.Value, Option[Long]) =
                  currentState(datasetRef: DatasetRef, shard: Int)

                override def updateInitState(datasetRef: DatasetRef, shard: Int, state: IndexState.Value, time: Long)
                :Unit = {

                }
              }))
            index.closeIndex()
          } catch {
            case is: IllegalStateException =>
              assert(is.getMessage.equals("Unable to clean up index directory"))

              events.toList match {
                case (IndexState.TriggerRebuild, _) :: Nil =>
                  // The file originally present would still be there as the directory
                  // is made readonly
                  assert(shardDirectory.list().exists(_.equals("empty")))
                  events.clear()
                case other                                                               =>
                  fail(s"Expected an index state Empty after directory cleanup - got ${other}")
              }
          } finally {
            shardDirectory.setWritable(true)
          }

      }
    }

    it("should match records without label when .* is provided on a non existent label") {

      val pkrs = partKeyFromRecords(dataset6, records(dataset6, readers.take(10)), Some(partBuilder))
        .zipWithIndex.map { case (addr, i) =>
          val pk = partKeyOnHeap(dataset6.schema.partKeySchema, ZeroPointer, addr)
          keyIndex.addPartKey(pk, i, i, i + 10)()
          PartKeyLuceneIndexRecord(pk, i, i + 10)
        }
      keyIndex.refreshReadersBlocking()


      // Query with just the existing Label name
      val filter1 = ColumnFilter("Actor2Code", Equals("GOV".utf8))
      val result1 = keyIndex.partKeyRecordsFromFilters(Seq(filter1), 0, Long.MaxValue)
      val expected1 = Seq(pkrs(7), pkrs(8), pkrs(9))

      result1.map(_.partKey.toSeq) should contain theSameElementsAs expected1.map(_.partKey.toSeq)
      result1.map(p => (p.startTime, p.endTime)) should contain theSameElementsAs expected1.map(p => (p.startTime, p.endTime))

      // Query with non existent label name with an empty regex
      val filter2 = ColumnFilter("dummy", EqualsRegex(".*".utf8))
      val filter3 = ColumnFilter("Actor2Code", Equals("GOV".utf8))
      val result2 = keyIndex.partKeyRecordsFromFilters(Seq(filter2, filter3), 0, Long.MaxValue)
      val expected2 = Seq(pkrs(7), pkrs(8), pkrs(9))

      result2.map(_.partKey.toSeq) should contain theSameElementsAs expected2.map(_.partKey.toSeq)
      result2.map(p => (p.startTime, p.endTime)) should contain theSameElementsAs expected2.map(p => (p.startTime, p.endTime))

      // Query with non existent label name with an regex matching at least 1 character
      val filter4 = ColumnFilter("dummy", EqualsRegex(".+".utf8))
      val filter5 = ColumnFilter("Actor2Code", Equals("GOV".utf8))
      val result3 = keyIndex.partKeyRecordsFromFilters(Seq(filter4, filter5), 0, Long.MaxValue)
      result3 shouldEqual Seq()

      // Query with non existent label name with an empty regex
      val filter6 = ColumnFilter("dummy", EqualsRegex("".utf8))
      val filter7 = ColumnFilter("Actor2Code", Equals("GOV".utf8))
      val result4 = keyIndex.partKeyRecordsFromFilters(Seq(filter6, filter7), 0, Long.MaxValue)
      val expected4 = Seq(pkrs(7), pkrs(8), pkrs(9))
      result4.map(_.partKey.toSeq) should contain theSameElementsAs expected4.map(_.partKey.toSeq)
      result4.map(p => (p.startTime, p.endTime)) should contain theSameElementsAs expected4.map(p => (p.startTime, p.endTime))

      // Query with non existent label name with an empty equals
      val filter8 = ColumnFilter("dummy", Equals("".utf8))
      val filter9 = ColumnFilter("Actor2Code", Equals("GOV".utf8))
      val result5 = keyIndex.partKeyRecordsFromFilters(Seq(filter8, filter9), 0, Long.MaxValue)
      val expected5 = Seq(pkrs(7), pkrs(8), pkrs(9))
      result5.map(_.partKey.toSeq) should contain theSameElementsAs expected5.map(_.partKey.toSeq)
      result5.map(p => (p.startTime, p.endTime)) should contain theSameElementsAs expected5.map(p => (p.startTime, p.endTime))


      val filter10 = ColumnFilter("Actor2Code", EqualsRegex(".*".utf8))
      val result10= keyIndex.partKeyRecordsFromFilters(Seq(filter10), 0, Long.MaxValue)
      result10.map(_.partKey.toSeq) should contain theSameElementsAs pkrs.map(_.partKey.toSeq)
      result10.map(p => (p.startTime, p.endTime)) should contain theSameElementsAs pkrs.map(p => (p.startTime, p.endTime))
    }

    it("should translate a part key to a part ID") {
      val pks = partKeyFromRecords(dataset6, records(dataset6, readers.take(10)), Some(partBuilder))
        .zipWithIndex.map { case (addr, i) =>
          val pk = partKeyOnHeap(dataset6.partKeySchema, ZeroPointer, addr)
          keyIndex.addPartKey(pk, i, System.currentTimeMillis())()

          pk
        }
      keyIndex.refreshReadersBlocking()

      val partKeyOpt = keyIndex.partIdFromPartKeySlow(pks(0), UnsafeUtils.arayOffset)
      partKeyOpt.isDefined shouldBe true
      partKeyOpt.get shouldEqual 0
    }
  }
  // scalastyle:on cyclomatic.complexity
  // scalastyle:on method.length
}
