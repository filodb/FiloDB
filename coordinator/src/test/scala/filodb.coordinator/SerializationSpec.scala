package filodb.coordinator

import akka.actor.{Address, ActorRef}
import java.io.{ByteArrayInputStream, ObjectInputStream}
import java.io.{ByteArrayOutputStream, ObjectOutputStream}

import filodb.core.binaryrecord.{BinaryRecord, RecordSchema}
import filodb.core.DatasetRef

import org.scalatest.{FunSpec, Matchers, BeforeAndAfter, BeforeAndAfterAll}

class SerializationSpec extends FunSpec with Matchers {
  import IngestionCommands._

  it("should be able to serialize different IngestionCommands messages") {
    val baos = new ByteArrayOutputStream
    val oos = new ObjectOutputStream(baos)
    val setupMsg = SetupIngestion(DatasetRef("foo"), Seq("colA", "colB"), 0)
    oos.writeObject(setupMsg)
    oos.writeObject(UnknownDataset)
    oos.writeObject(BadSchema("no match foo blah"))
    oos.writeObject(Ack(123L))

    val ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray))
    ois.readObject should equal (setupMsg)
    ois.readObject should equal (UnknownDataset)
    ois.readObject should equal (BadSchema("no match foo blah"))
    ois.readObject should equal (Ack(123L))
  }

  it("should be able to serialize a PartitionMapper") {
    val baos = new ByteArrayOutputStream
    val oos = new ObjectOutputStream(baos)
    val emptyRef = ActorRef.noSender
    val addr1 = Address("http", "test", "host1", 1001)
    val mapper = PartitionMapper.empty.addNode(addr1, emptyRef)
    oos.writeObject(mapper)

    val ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray))
    ois.readObject should equal (mapper)
  }

  it("should be able to serialize and deserialize IngestRows with BinaryRecords") {
    import filodb.core.NamesTestData._
    import Serializer._

    val binSchema = RecordSchema(schema)
    putSchema(binSchema)
    val records = mapper(names).map { r => BinaryRecord(binSchema, r) }
    val cmd = IngestRows(datasetRef, 0, records, 100L)
    fromBinaryIngestRows(cmd.toBytes()) should equal (cmd)
  }
}