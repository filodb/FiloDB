package filodb.core.query

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.metadata.Schemas
import filodb.memory.MemFactory
import filodb.memory.format.ZeroCopyUTF8String
import filodb.memory.format.ZeroCopyUTF8String._

class SelectLabelsSpec extends AnyFunSpec with Matchers {

  // mixes predefined keys (_ws_, _ns_, instance) with non-predefined ones (foo, zone)
  private val seriesTags = Map("_ws_".utf8 -> "my_ws".utf8, "_ns_".utf8 -> "my_ns".utf8,
                               "instance".utf8 -> "host1".utf8, "foo".utf8 -> "bar".utf8, "zone".utf8 -> "z1".utf8)

  private val partKey: PartitionRangeVectorKey = {
    val schema = Schemas.promCounter
    val builder = new RecordBuilder(MemFactory.onHeapFactory)
    val offset = builder.partKeyFromObjects(schema, "my_metric".utf8, seriesTags)
    val base = builder.allContainers.head.base.asInstanceOf[Array[Byte]]
    PartitionRangeVectorKey(Right((base, offset)), schema.partKeySchema, schema.partition.columns.map(ColumnInfo(_)),
      0, 0, 0, schema.name)
  }

  private val brMapKey: BrMapRangeVectorKey = {
    val builder = new RecordBuilder(MemFactory.onHeapFactory)
    partKey.writeToMapBr(builder)
    val container = builder.allContainers.head
    BrMapRangeVectorKey(container.base, container.allOffsets(0))
  }

  private val customKey = CustomRangeVectorKey(partKey.labelValues)

  private def labels(names: String*): Array[ZeroCopyUTF8String] = names.map(_.utf8).toArray

  private val labelSets = Seq(
    labels(), labels("instance"), labels("_metric_"), labels("foo", "instance"),
    labels("_ws_", "_ns_", "zone"), labels("missing"), labels("instance", "missing"),
    labels("_metric_", "_ws_", "_ns_", "instance", "foo", "zone"))

  describe("RangeVectorKey.selectLabels") {
    it("should decode all labels of the part key in the test fixtures") {
      partKey.labelValues shouldEqual seriesTags + ("_metric_".utf8 -> "my_metric".utf8)
      brMapKey.labelValues shouldEqual partKey.labelValues
    }

    it("should keep only the clause labels for a by clause") {
      partKey.selectLabels(labels("instance", "foo", "missing"), include = true) shouldEqual
        Map("instance".utf8 -> "host1".utf8, "foo".utf8 -> "bar".utf8)
      partKey.selectLabels(labels("_metric_"), include = true) shouldEqual Map("_metric_".utf8 -> "my_metric".utf8)
      partKey.selectLabels(labels(), include = true) shouldEqual Map.empty[ZeroCopyUTF8String, ZeroCopyUTF8String]
    }

    it("should drop the clause labels for a without clause") {
      partKey.selectLabels(labels("instance", "_metric_", "missing"), include = false) shouldEqual
        seriesTags - "instance".utf8
      partKey.selectLabels(labels(), include = false) shouldEqual partKey.labelValues
    }

    it("should match filtering the full labelValues for every key type") {
      for {
        key <- Seq[RangeVectorKey](partKey, brMapKey, customKey)
        ls <- labelSets
        include <- Seq(true, false)
      } {
        withClue(s"key=${key.getClass.getSimpleName} labels=${ls.mkString(",")} include=$include: ") {
          key.selectLabels(ls, include) shouldEqual key.labelValues.filter(lv => ls.contains(lv._1) == include)
        }
      }
    }

    it("should reuse the clause label instances as keys for a by clause") {
      val ls = labels("instance", "foo")
      val selected = partKey.selectLabels(ls, include = true)
      selected.keys.foreach(k => ls.exists(_ eq k) shouldEqual true)
    }
  }
}
