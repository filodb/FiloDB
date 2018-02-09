package com.esotericsoftware.kryo.io

import java.nio.ByteBuffer

import com.esotericsoftware.kryo.{Serializer => KryoSerializer}
import com.esotericsoftware.kryo.Kryo
import com.typesafe.scalalogging.StrictLogging

import filodb.core.binaryrecord.{BinaryRecord, RecordSchema}
import filodb.memory.format.{BinaryVector, FiloVector, UnsafeUtils, VectorReader}

// NOTE: This file has to be in the kryo namespace so we can use the require() method

/**
 * A Kryo serializer for BinaryVectors.  Since BinaryVectors are already blobs, the goal here is to ship bytes
 * out as quickly as possible.  Standard field-based serializers must NOT be used here.
 * TODO: optimize this code for input/output classes like UnsafeMemoryInput/Output.  Right now we have to assume
 * a generic Input/Output which uses a byte[] onheap buffer, and copy stuff in and out of that.  This is obviously
 * less ideal.
 */
class BinaryVectorSerializer[A: VectorReader] extends KryoSerializer[BinaryVector[A]] with StrictLogging {
  override def read(kryo: Kryo, input: Input, typ: Class[BinaryVector[A]]): BinaryVector[A] = {
    val onHeapBuffer = ByteBuffer.wrap(input.readBytes(input.readInt))
    logger.trace(s"Reading typ=$typ with reader=${implicitly[VectorReader[A]]} into buffer $onHeapBuffer")
    FiloVector[A](onHeapBuffer).asInstanceOf[BinaryVector[A]]
  }

  override def write(kryo: Kryo, output: Output, vector: BinaryVector[A]): Unit = {
    val buf = vector.toFiloBuffer         // now idempotent, can be called many times
    var bytesToGo = vector.numBytes + 4   // include the header bytes
    output.writeInt(bytesToGo)
    output.require(bytesToGo)
    buf.get(output.getBuffer, output.position, bytesToGo)
    output.setPosition(output.position + bytesToGo)
  }
}

/**
 * Serializer for BinaryRecords.  One complication with BinaryRecords is that they require a schema.
 * We don't want to instantiate a new RecordSchema with every single BR, that would be a huge waste of memory.
 * However, it seems that Kryo remembers RecordSchema references, and if the same RecordSchema is used for multiple
 * BinaryRecords in an object graph (say a VectorListResult or TupleListResult) then it will be stored by some
 * reference ID.  Thus it saves us cost and memory allocations on restore.  :)
 */
class BinaryRecordSerializer extends KryoSerializer[BinaryRecord] with StrictLogging {
  override def read(kryo: Kryo, input: Input, typ: Class[BinaryRecord]): BinaryRecord = {
    val schema = kryo.readObject(input, classOf[RecordSchema])
    val bytes = input.readBytes(input.readInt)
    BinaryRecord(schema, bytes)
  }

  override def write(kryo: Kryo, output: Output, br: BinaryRecord): Unit = {
    kryo.writeObject(output, br.schema)
    output.writeInt(br.numBytes)
    // It would be simpler if we simply took the bytes from ArrayBinaryRecord and wrote them, but
    // BinaryRecords might go offheap.
    output.require(br.numBytes)
    br.copyTo(output.getBuffer, UnsafeUtils.arayOffset + output.position)
    output.setPosition(output.position + br.numBytes)
  }
}