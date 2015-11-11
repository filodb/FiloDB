package filodb.core.reprojector

import com.typesafe.config.ConfigFactory
import filodb.core.KeyRange
import filodb.core.columnstore.{InMemoryColumnStore, SegmentChopper, SegmentSpec}
import filodb.core.metadata.RichProjection
import org.velvia.filo.TupleRowReader

import org.scalatest.{FunSpec, Matchers, BeforeAndAfter}
import org.scalatest.concurrent.ScalaFutures

/**
 * This is kind of close to an end to end spec for core. It involves memtable, reprojector,
 * SegmentChopper, Segment, InMemoryColumnStore, RichProjection.
 */
class ReprojectorSpec extends FunSpec with Matchers with BeforeAndAfter with ScalaFutures {
  import SegmentSpec._

  import scala.concurrent.ExecutionContext.Implicits.global
  val columnStore = new InMemoryColumnStore
  val config = ConfigFactory.load("application_test.conf")
  val lotLotNamesProjection = RichProjection[Long](largeDataset, schemaWithPartCol)
  var resp = 0

  before {
    columnStore.clearProjectionData(dataset.projections.head)
  }

  it("toSegments should convert all rows to segments given all keyranges") {
    val memTable = new MapDBMemTable(lotLotNamesProjection, config)
    memTable.ingestRows(lotLotNames.map(TupleRowReader)) { resp = 2 }
    resp should equal (2)

    val (metaMap, uuidMap) = SegmentChopper.loadSegmentInfos(projection, memTable.partitions.toSeq,
                                                             0, columnStore).futureValue
    val chopper = new SegmentChopper(lotLotNamesProjection, metaMap, 200, 300)
    chopper.insertOrderedKeys(memTable.allKeys)

    val reprojector = new DefaultReprojector(columnStore)
    val segments = reprojector.toSegments(memTable, chopper.keyRanges).toSeq

    segments should have length (6)
    val segRanges = segments.map(_.keyRange)
    // Remember that keyRanges are sorted by ascending partition key = afc, nfc
    segRanges.map(_.partition) should equal (Seq("afc", "afc", "afc", "nfc", "nfc", "nfc"))
    segRanges.map(_.start) should equal (Seq(None, Some(10100L), Some(20100L),
                                             None, Some(10100L), Some(20100L)))
    segments.map(_.index.rowNumIterator.length).sum should equal (1200)
  }

  it("toSegments should convert only row in select keyranges") {
    val memTable = new MapDBMemTable(lotLotNamesProjection, config)
    memTable.ingestRows(lotLotNames.map(TupleRowReader)) { resp = 2 }
    resp should equal (2)

    val (metaMap, uuidMap) = SegmentChopper.loadSegmentInfos(projection, memTable.partitions.toSeq,
                                                             0, columnStore).futureValue
    val chopper = new SegmentChopper(lotLotNamesProjection, metaMap, 200, 300)
    chopper.insertOrderedKeys(memTable.allKeys)

    // Filter to only two out of 6 keyRanges
    val keyRanges = chopper.keyRanges.filter { _.start.getOrElse(0L) > 20000L }
    val reprojector = new DefaultReprojector(columnStore)
    val segments = reprojector.toSegments(memTable, keyRanges).toSeq

    segments should have length (2)
    val segRanges = segments.map(_.keyRange)
    // Remember that keyRanges are sorted by ascending partition key = afc, nfc
    segRanges.map(_.partition) should equal (Seq("afc", "nfc"))
    segRanges.map(_.start) should equal (Seq(Some(20100L), Some(20100L)))
    segments.map(_.index.rowNumIterator.length).sum should equal (400)
  }

  it("reproject() should flush segments to column store") {
    val memTable = new MapDBMemTable(lotLotNamesProjection, config)
    memTable.ingestRows(lotLotNames.map(TupleRowReader)) { resp = 2 }
    resp should equal (2)

    val (metaMap, uuidMap) = SegmentChopper.loadSegmentInfos(projection, memTable.partitions.toSeq,
                                                             0, columnStore).futureValue
    val chopper = new SegmentChopper(lotLotNamesProjection, metaMap, 200, 300)
    chopper.insertOrderedKeys(memTable.allKeys)

    val keyRanges = chopper.keyRanges
    val reprojector = new DefaultReprojector(columnStore)
    val writtenRanges = reprojector.reproject(memTable, 0, keyRanges).futureValue

    writtenRanges should equal (keyRanges)
    val segs = columnStore.readSegments(schemaWithPartCol.drop(2), keyRanges(1), 0).futureValue.toSeq
    segs should have length (1)
    val segRanges = segs.map(_.keyRange)
    // Remember that keyRanges are sorted by ascending partition key = afc, nfc
    segRanges.map(_.partition) should equal (Seq("afc"))
    segRanges.map(_.start) should equal (Seq(Some(10100L)))
    segs.map(_.index.rowNumIterator.length).sum should equal (200)
  }
}