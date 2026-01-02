package filodb.core

import com.esotericsoftware.kryo.{Kryo, Serializer => KryoSerializer}
import com.esotericsoftware.kryo.io.{Input, Output}

import filodb.core.binaryrecord2.{RecordSchema => RecordSchema2}
import filodb.core.metadata.Column
import filodb.core.query.ColumnInfo

class RecordSchema2Serializer extends KryoSerializer[RecordSchema2] {
  override def read(kryo: Kryo, input: Input, typ: Class[RecordSchema2]): RecordSchema2 = {
    val tuple = kryo.readClassAndObject(input)
    RecordSchema2.fromSerializableTuple(tuple.asInstanceOf[(Seq[ColumnInfo], Option[Int],
      Seq[String], Map[Int, RecordSchema2])])
  }

  override def write(kryo: Kryo, output: Output, schema: RecordSchema2): Unit = {
    kryo.writeClassAndObject(output, schema.toSerializableTuple)
  }
}

// All the ColumnTypes are Objects - singletons.  Thus the class info is enough to find the right one.
// No need to actually write anything.  :D :D :D
class ColumnTypeSerializer extends KryoSerializer[Column.ColumnType] {
  override def read(kryo: Kryo, input: Input, typ: Class[Column.ColumnType]): Column.ColumnType =
    Column.clazzToColType(typ)

  override def write(kryo: Kryo, output: Output, colType: Column.ColumnType): Unit = {}
}