package com.esotericsoftware.kryo.io

import com.esotericsoftware.kryo.{Serializer => KryoSerializer}
import com.esotericsoftware.kryo.Kryo
import com.typesafe.config.{ ConfigFactory, ConfigRenderOptions}
import com.typesafe.scalalogging.StrictLogging

import filodb.core.binaryrecord2.{RecordSchema => RecordSchema2}
import filodb.core.query.{ColumnInfo, PartitionInfo, PartitionRangeVectorKey, PromQlQueryParams}
import filodb.memory.format._

// NOTE: This file has to be in the kryo namespace so we can use the require() method

object BinaryRegionUtils extends StrictLogging {
  def writeLargeRegion(base: Any, offset: Long, output: Output): Unit = {
    val numBytes = UnsafeUtils.getInt(base, offset)
    output.writeInt(numBytes)
    output.require(numBytes)
    UnsafeUtils.unsafe.copyMemory(base, offset + 4, output.getBuffer,
                                  UnsafeUtils.arayOffset + output.position, numBytes)
    output.setPosition(output.position + numBytes)
    logger.trace(s"Wrote large region of $numBytes bytes...")
  }

  def readLargeRegion(input: Input): Array[Byte] = {
    val regionLen = input.readInt
    val bytes = new Array[Byte](regionLen + 4)
    val bytesRead = input.read(bytes, 4, regionLen)
    UnsafeUtils.setInt(bytes, UnsafeUtils.arayOffset, regionLen)
    logger.trace(s"Read large region of $regionLen bytes: ${bytes.toSeq.take(20)}")
    bytes
  }
}

class PartitionRangeVectorKeySerializer extends KryoSerializer[PartitionRangeVectorKey] with StrictLogging {
  override def read(kryo: Kryo, input: Input, typ: Class[PartitionRangeVectorKey]): PartitionRangeVectorKey = {
    val partBytes = BinaryRegionUtils.readLargeRegion(input)
    val schema = kryo.readObject(input, classOf[RecordSchema2])
    val keyCols = kryo.readClassAndObject(input)
    PartitionRangeVectorKey(partBytes, UnsafeUtils.arayOffset,
      schema, keyCols.asInstanceOf[Seq[ColumnInfo]], input.readInt, input.readInt, input.readInt)
  }

  override def write(kryo: Kryo, output: Output, key: PartitionRangeVectorKey): Unit = {
    BinaryRegionUtils.writeLargeRegion(key.partBase, key.partOffset, output)
    kryo.writeObject(output, key.partSchema)
    kryo.writeClassAndObject(output, key.partKeyCols)
    output.writeInt(key.sourceShard)
    output.writeInt(key.groupNum)
    output.writeInt(key.partId)
  }
}

class ZeroCopyUTF8StringSerializer extends KryoSerializer[ZeroCopyUTF8String] with StrictLogging {
  override def read(kryo: Kryo, input: Input, typ: Class[ZeroCopyUTF8String]): ZeroCopyUTF8String = {
    val numBytes = input.readInt
    ZeroCopyUTF8String(input.readBytes(numBytes))
  }

  override def write(kryo: Kryo, output: Output, key: ZeroCopyUTF8String): Unit = {
    output.writeInt(key.numBytes)
    output.require(key.numBytes)
    key.copyTo(output.getBuffer, UnsafeUtils.arayOffset + output.position)
    output.setPosition(output.position + key.numBytes)
  }
}

class PartitionInfoSerializer extends KryoSerializer[PartitionInfo] {
  override def read(kryo: Kryo, input: Input, typ: Class[PartitionInfo]): PartitionInfo = {
    val schema = kryo.readObject(input, classOf[RecordSchema2])
    val partBytes = BinaryRegionUtils.readLargeRegion(input)
    PartitionInfo(schema, partBytes, UnsafeUtils.arayOffset, input.readInt)
  }

  override def write(kryo: Kryo, output: Output, info: PartitionInfo): Unit = {
    kryo.writeObject(output, info.schema)
    BinaryRegionUtils.writeLargeRegion(info.base, info.offset, output)
    output.writeInt(info.shardNo)
  }
}

class PromQlQueryParamsSerializer extends KryoSerializer[PromQlQueryParams] {
  override def read(kryo: Kryo, input: Input, typ: Class[PromQlQueryParams]): PromQlQueryParams = {
    val config = ConfigFactory.parseString(input.readString())
    val promQl = input.readString()
    val start = input.readLong()
    val step = input.readLong()
    val end = input.readLong()
    val spreadInt = input.readInt()
    val spread = if (spreadInt == -1) None else Some(spreadInt)
    val procFailure = input.readBoolean()
    PromQlQueryParams(config, promQl, start, step, end, spread, procFailure)
  }
  override def write(kryo: Kryo, output: Output, promParam: PromQlQueryParams): Unit = {
    output.writeString(promParam.config.root().render(ConfigRenderOptions.concise()))
    output.writeString(promParam.promQl)
    output.writeLong(promParam.startSecs)
    output.writeLong(promParam.stepSecs)
    output.writeLong(promParam.endSecs)
    output.writeInt(promParam.spread.getOrElse(-1))
    output.writeBoolean(promParam.processFailure)
  }
}