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
}
