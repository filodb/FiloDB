package filodb.coordinator

import com.typesafe.scalalogging.StrictLogging
import org.boon.primitive.{ByteBuf, InputByteArray}
import org.velvia.filo.RowReader

import filodb.core._
import filodb.core.binaryrecord.{BinaryRecord, RecordSchema}
import filodb.coordinator.IngestionCommands.IngestRows

/**
 * Utilities for special serializers for row data (for efficient record routing).
 *
 * We also maintain some global state for mapping a schema hashcode to the actual schema object.  This must
 * be updated externally.  The reason this is decided is because IngestRows is sent between a RowSource
 * client and node ingestor actors, both of whom have exchanged and know the schema already, thus there is
 * no need to pay the price of transmitting the schema itself with every IngestRows object.
 */
object Serializer extends StrictLogging {
  implicit class RichIngestRows(data: IngestRows) {
    /**
     * Converts a IngestRows into bytes, assuming all RowReaders are in fact BinaryRecords
     */
    def toBytes(): Array[Byte] =
      serializeIngestRows(data)
  }

  def serializeIngestRows(data: IngestRows): Array[Byte] = {
    val buf = ByteBuf.create(1000)
    val schemaHash = data.rows.headOption match {
      case Some(b: BinaryRecord) => b.schema.hashCode
      case other: Any            => -1
    }
    buf.writeInt(schemaHash)
    buf.writeMediumString(data.dataset.dataset)
    buf.writeMediumString(data.dataset.database.getOrElse(""))
    buf.writeInt(data.version)
    buf.writeLong(data.seqNo)
    buf.writeInt(data.rows.length)
    for { record <- data.rows } {
      record match {
        case b: BinaryRecord => buf.writeMediumByteArray(b.bytes)
      }
    }
    buf.toBytes
  }

  def fromBinaryIngestRows(bytes: Array[Byte]): IngestRows = {
    val scanner = new InputByteArray(bytes)
    val schemaHash = scanner.readInt
    val dataset = scanner.readMediumString
    val db = Option(scanner.readMediumString).filter(_.length > 0)
    val version = scanner.readInt
    val seqNo = scanner.readLong
    val numRows = scanner.readInt
    val rows = new collection.mutable.ArrayBuffer[BinaryRecord]()
    if (numRows > 0) {
      val schema = schemaMap.get(schemaHash)
      if (Option(schema).isEmpty) { logger.error(s"Schema with hash $schemaHash not found!") }
      for { i <- 0 until numRows } {
        rows += BinaryRecord(schema, scanner.readMediumByteArray)
      }
    }
    IngestionCommands.IngestRows(DatasetRef(dataset, db), version, rows, seqNo)
  }

  private val schemaMap = new java.util.concurrent.ConcurrentHashMap[Int, RecordSchema]

  def putSchema(schema: RecordSchema): Unit = {
    logger.debug(s"Saving schema with fields ${schema.fields.toList} and hash ${schema.hashCode}...")
    schemaMap.put(schema.hashCode, schema)
  }
}

/**
 * A special serializer to use the fast binary IngestRows format instead of much slower Java serialization
 * To solve the problem of the serializer not knowing the schema beforehand, it is the responsibility of
 * the DatasetCoordinatorActor to update the schema when it changes.  The hash of the RecordSchema is
 * sent and received.
 */
class IngestRowsSerializer extends akka.serialization.Serializer {
  import Serializer._

  // This is whether "fromBinary" requires a "clazz" or not
  def includeManifest: Boolean = false

  def identifier: Int = 1001

  def toBinary(obj: AnyRef): Array[Byte] = obj match {
    case i: IngestRows => i.toBytes()
  }

  def fromBinary(bytes: Array[Byte],
                 clazz: Option[Class[_]]): AnyRef = {
    fromBinaryIngestRows(bytes)
  }
}