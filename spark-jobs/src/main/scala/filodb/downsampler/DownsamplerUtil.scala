package filodb.downsampler

import scala.collection.mutable

import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.metadata.Schema
import filodb.downsampler.index.DSIndexJob.schemas
import filodb.memory.{BinaryRegionLarge, MemFactory}
import filodb.memory.format.{UnsafeUtils, ZeroCopyUTF8String}
import filodb.memory.format.ZeroCopyUTF8String._

object DownsamplerUtil {

  /**
    * create downsampled Partkey from source/raw Partkey
    */
  def downsampledPk(partKey: Array[Byte], dsSchema: Schema, offHeapMem: MemFactory): Array[Byte] = {
    val pkStrPairs = schemas.part.binSchema.toStringPairs(partKey, UnsafeUtils.arayOffset)
    downsampledPk(pkStrPairs, dsSchema, offHeapMem)
  }

  /**
    * create downsampled Partkey from the source/raw Partkey
    */
  def downsampledPk(pkStrPairs: Seq[(String, String)], dsSchema: Schema, offHeapMem: MemFactory): Array[Byte] = {
    val dsRecordBuilder = new RecordBuilder(offHeapMem)
    val metric = pkStrPairs.find(_._1 == dsSchema.options.metricColumn).get._2
    val map: mutable.HashMap[ZeroCopyUTF8String, ZeroCopyUTF8String] = mutable.HashMap.empty
    pkStrPairs.filter(_._1 != dsSchema.options.metricColumn).foreach(a => map.put(a._1.utf8, a._2.utf8))
    val partKeyOffset = dsRecordBuilder.partKeyFromObjects(dsSchema, metric, map.toMap)
    BinaryRegionLarge.asNewByteArray(partKeyOffset)
  }

}
