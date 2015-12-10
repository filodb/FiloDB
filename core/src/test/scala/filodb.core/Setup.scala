package filodb.core

import java.nio.ByteBuffer

import filodb.core.metadata.{Column, KeyRange}
import filodb.core.store.Dataset
import org.velvia.filo.{FastFiloRowReader, FiloRowReader, RowReader, TupleRowReader}

object Setup {
  type RowReaderFactory = (Array[ByteBuffer], Array[Class[_]]) => FiloRowReader
  val readerFactory: RowReaderFactory = (bytes, classes) => new FastFiloRowReader(bytes, classes)

  val schema = Seq(
    Column("country", "dataset", 0, Column.ColumnType.StringColumn),
    Column("city", "dataset", 0, Column.ColumnType.StringColumn),
    Column("first", "dataset", 0, Column.ColumnType.StringColumn),
    Column("last", "dataset", 0, Column.ColumnType.StringColumn),
    Column("age", "dataset", 0, Column.ColumnType.LongColumn))

  def mapper(rows: Seq[Product]): Iterator[RowReader] = rows.map(TupleRowReader).toIterator

  // primary key and segment are same
  val dataset = Dataset("dataset", schema, "country", "first", "age", "city")
  val projection = dataset.projections.head
  val keyRange = KeyRange(Some("A"), Some("Z"))
  val DefaultPartitionKey = "Single"

  val names = Seq(
    (Some("US"), Some("SF"), Some("Khalil"), Some("Mack"), Some(24L)),
    (Some("US"), Some("NY"), Some("Ndamukong"), Some("Suh"), Some(28L)),
    (Some("US"), Some("NY"), Some("Rodney"), Some("Hudson"), Some(25L)),
    (Some("UK"), Some("LN"), Some("Jerry"), None, Some(40L)),
    (Some("UK"), Some("LN"), Some("Peyton"), Some("Manning"), Some(39L)),
    (Some("UK"), Some("LN"), Some("Terrance"), Some("Knighton"), Some(29L)))

  val names2 = Seq(
    (Some("US"), Some("SF"), Some("Khalil"), Some("Khadri"), Some(24L)),
    (Some("US"), Some("NY"), Some("Bradley"), Some("Hudson"), Some(25L)),
    (Some("UK"), Some("LN"), Some("Peyton"), Some("Manning"), Some(50L)),
    (Some("UK"), Some("LN"), Some("Terrance"),  Some("Parr"), Some(29L)),
    (Some("UK"), Some("LN"), Some("Helen"),     Some("Troy"), Some(29L)))

  val names3 = Seq(
    (Some("US"), Some("SF"), Some("Ahmed"), Some("Khadri"), Some(24L)),
    (Some("US"), Some("NY"), Some("Casey"), Some("Hudson"), Some(25L)),
    (Some("UK"), Some("LN"), Some("Peyton"), Some("Manning"), Some(40L)),
    (Some("UK"), Some("LN"), Some("Terrance"),  Some("Parr"), Some(39L)),
    (Some("UK"), Some("LN"), Some("Cassandra"),     Some("Troy"), Some(29L)))

  val firstNames = Seq("Khalil", "Ndamukong", "Rodney", "Terrance", "Peyton", "Jerry")


}
