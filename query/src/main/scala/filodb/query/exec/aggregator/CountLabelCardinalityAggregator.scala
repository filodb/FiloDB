package filodb.query.exec.aggregator

import filodb.core.query._
import filodb.memory.format.{RowReader, UnsafeUtils}

class CountLabelCardinalityAggregator extends RowAggregator {

  class CountLabelCardinalityHolder(var timestamp: Long = 0L) extends AggregateHolder {

    // Created once and reused to avoid GC, If needed a bigger array will be created and held and not shrunk back
    var serializedMap = new Array[Byte](500)

    private val sketchMap = debox.Map[String, Array[Byte]]()
    val row = new LabelCardinalityCountTransientRow

    override def resetToZero(): Unit = sketchMap.clear()

    override def toRowReader: MutableRowReader = {
      val size = LabelCardinalityMapSerDeser.serializedSize(sketchMap)
      if (serializedMap.length < size) serializedMap = new Array[Byte](size) // Create bigger array if needed
      LabelCardinalityMapSerDeser.serialize(sketchMap, serializedMap)
      row.setLong(0, timestamp)
      row.setBlob(1, serializedMap, UnsafeUtils.arayOffset, size)
      row
    }
  }

  override type AggHolderType = CountLabelCardinalityHolder


  override def zero: CountLabelCardinalityHolder = new CountLabelCardinalityHolder()

  override def newRowToMapInto: MutableRowReader = new LabelCardinalityCountTransientRow()

  override def map(rvk: RangeVectorKey, item: RowReader, mapInto: MutableRowReader): RowReader = ???

  override def reduceAggregate(acc: CountLabelCardinalityHolder, aggRes: RowReader): CountLabelCardinalityHolder = ???

  override def present(aggRangeVector: RangeVector, limit: Int, rangeParams: RangeParams): Seq[RangeVector] = ???

  override def reductionSchema(source: ResultSchema): ResultSchema = ???

  override def presentationSchema(source: ResultSchema): ResultSchema = ???
}
