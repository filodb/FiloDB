package filodb.core.memstore

import filodb.core.GdeltTestData.dataset6
import filodb.core.{DatasetRef, TestData}
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.metadata.PartitionSchema
import filodb.core.query.ColumnFilter
import filodb.core.query.Filter.{Equals, EqualsRegex, In}
import org.scalatest.BeforeAndAfter
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import java.io.File

class PartKeyTantivyIndexSpec extends AnyFunSpec with Matchers with BeforeAndAfter with PartKeyIndexRawSpec {
  val keyIndex = new PartKeyTantivyIndex(dataset6.ref, dataset6.schema.partition, 0, 1.hour.toMillis,
    Some(new File(System.getProperty("java.io.tmpdir"), "part-key-lucene-index")))

  val partBuilder = new RecordBuilder(TestData.nativeMem)

  before {
    keyIndex.reset()
    keyIndex.refreshReadersBlocking()
  }

  after {
    partBuilder.removeAndFreeContainers(partBuilder.allContainers.length)
  }

  protected def createNewIndex(ref: DatasetRef,
                               schema: PartitionSchema,
                               facetEnabledAllLabels: Boolean,
                               facetEnabledShardKeyLabels: Boolean,
                               shardNum: Int,
                               retentionMillis: Long,
                               diskLocation: Option[File],
                               lifecycleManager: Option[IndexMetadataStore]): PartKeyIndexRaw = {
    new PartKeyTantivyIndex(ref, schema, shardNum, retentionMillis, diskLocation, lifecycleManager)
  }

  it should behave like commonPartKeyTests(keyIndex, partBuilder)

  it("should encode equals queries correctly") {
    val builder = new TantivyQueryBuilder()

    // Simple equals filter
    val filters = List(ColumnFilter("col1", Equals("abcd")))
    val query = builder.buildQuery(filters)

    query should contain theSameElementsInOrderAs List(1,// Boolean
      1, // Must
      2, // Equals
      4, 0, // Length 4
      99, 111, 108, 49, // col1
      4, 0, // Length 4
      97, 98, 99, 100, // abcd
      0) // End boolean
  }

  it("should encode equals regex correctly") {
    val builder = new TantivyQueryBuilder()

    // Simple equals filter
    val filters = List(ColumnFilter("col1", EqualsRegex("a.*b")))
    val query = builder.buildQuery(filters)

    query should contain theSameElementsInOrderAs List(1,// Boolean
      1, // Must
      3, // Regex
      4, 0, // Length 4
      99, 111, 108, 49, // col1
      4, 0, // Length 4
      97, 46, 42, 98, // a.*b
      0) // End boolean
  }

  it("should encode term in correctly") {
    val builder = new TantivyQueryBuilder()

    // Simple equals filter
    val filters = List(ColumnFilter("col1", In(Set("a","b"))))
    val query = builder.buildQuery(filters)

    query should contain theSameElementsInOrderAs List(1,// Boolean
      1, // Must
      4, // Term In
      4, 0, // Length 4
      99, 111, 108, 49, // col1
      2, 0, // Term count 2
      1, 0, // Length 1
      97, // a
      1, 0, // Length 1
      98, // b
      0) // End boolean
  }

  it("should encode prefix correctly") {
    val builder = new TantivyQueryBuilder()

    // Simple equals filter
    val filters = List(ColumnFilter("col1", EqualsRegex("a.*")))
    val query = builder.buildQuery(filters)

    query should contain theSameElementsInOrderAs List(1,// Boolean
      1, // Must
      5, // Prefix
      4, 0, // Length 4
      99, 111, 108, 49, // col1
      1, 0, // Length 1
      97, // a
      0) // End boolean
  }

  it("should encode match all correctly") {
    val builder = new TantivyQueryBuilder()

    // Simple equals filter
    val filters = List(ColumnFilter("col1", EqualsRegex(".*")))
    val query = builder.buildQuery(filters)

    query should contain theSameElementsInOrderAs List(1,// Boolean
      1, // Must
      6, // Match All
      0) // End boolean
  }

  it("should encode start and end time properly") {
    val builder = new TantivyQueryBuilder()

    // Simple equals filter
    val filters = List(ColumnFilter("col1", EqualsRegex(".*")))
    val query = builder.buildQueryWithStartAndEnd(filters, 1, Long.MaxValue)

    query should contain theSameElementsInOrderAs List(1,// Boolean
      1, // Must
      6, // Match All
      1, // Must
      7, // Long Range
      11, 0, 95, 95, 101, 110, 100, 84, 105, 109, 101, 95, 95, // __endTime__
      1, 0, 0, 0, 0, 0, 0, 0, // 0x1
      -1, -1, -1, -1, -1, -1, -1, 127, // Long.MAX_VALUE
      0) // End boolean
  }

  it("should filter correctly when a label has the same name as the map column") {
    import filodb.core.MachineMetricsData
    import filodb.memory.format.ZeroCopyUTF8String.StringToUTF8

    // dataset2 has partition schema: Seq("series:string", "tags:map")
    // This reproduces the production schema where the MapColumn is named "tags"
    val ds = MachineMetricsData.dataset2
    val tmpDir = new File(System.getProperty("java.io.tmpdir"), "part-key-tantivy-map-collision-test")
    val mapIndex = new PartKeyTantivyIndex(ds.ref, ds.schema.partition, 0, 1.hour.toMillis, Some(tmpDir))

    try {
      mapIndex.reset()
      mapIndex.refreshReadersBlocking()

      val partBuilder2 = new RecordBuilder(TestData.nativeMem)

      // Create a partition key where the map includes a "tags" entry (same name as the MapColumn)
      val tagsMap = Map("tags".utf8 -> "b1.metal".utf8,
                        "cell".utf8 -> "A".utf8,
                        "region".utf8 -> "us-central-2".utf8)
      val seriesName = "test_metric"

      partBuilder2.partKeyFromObjects(ds.schema, seriesName, tagsMap)
      val container = partBuilder2.allContainers.head
      val partKeyAddr = container.allOffsets(0)
      val partKeyBytes = ds.partKeySchema.asByteArray(container.base, partKeyAddr)

      val start = System.currentTimeMillis()
      mapIndex.addPartKey(partKeyBytes, 0, start)()
      mapIndex.refreshReadersBlocking()

      val end = System.currentTimeMillis()

      // Filtering by "cell" (a regular label, not colliding with MapColumn name) should work
      val cellFilter = Seq(ColumnFilter("cell", Equals("A")))
      val cellResults = mapIndex.partIdsFromFilters(cellFilter, start, end)
      cellResults.length shouldEqual 1

      // Filtering by "tags" (same name as the MapColumn) should also work
      val tagsFilter = Seq(ColumnFilter("tags", Equals("b1.metal")))
      val tagsResults = mapIndex.partIdsFromFilters(tagsFilter, start, end)
      tagsResults.length shouldEqual 1

      // Filtering by "tags" with a non-matching value should return no results
      val tagsMissFilter = Seq(ColumnFilter("tags", Equals("nonexistent")))
      val tagsMissResults = mapIndex.partIdsFromFilters(tagsMissFilter, start, end)
      tagsMissResults.length shouldEqual 0

      // Filtering by "tags" with regex should also work
      val tagsRegexFilter = Seq(ColumnFilter("tags", EqualsRegex("b1\\..*")))
      val tagsRegexResults = mapIndex.partIdsFromFilters(tagsRegexFilter, start, end)
      tagsRegexResults.length shouldEqual 1

    } finally {
      mapIndex.closeIndex()
      partBuilder.removeAndFreeContainers(partBuilder.allContainers.length)
    }
  }
}
