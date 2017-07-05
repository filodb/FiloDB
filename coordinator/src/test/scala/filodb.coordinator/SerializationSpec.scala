package filodb.coordinator

import akka.actor.{Address, ActorRef}
import java.io.{ByteArrayInputStream, ObjectInputStream}
import java.io.{ByteArrayOutputStream, ObjectOutputStream}

import filodb.core.binaryrecord.{BinaryRecord, RecordSchema}
import filodb.core.memstore.IngestRecord
import filodb.core.NamesTestData

import org.scalatest.{FunSpec, Matchers, BeforeAndAfter, BeforeAndAfterAll}

class SerializationSpec extends FunSpec with Matchers {
  import IngestionCommands._
  import NodeClusterActor._
  import NamesTestData._

  it("should be able to serialize different IngestionCommands messages") {
    val baos = new ByteArrayOutputStream
    val oos = new ObjectOutputStream(baos)
    val setupMsg = DatasetSetup(dataset, schema.map(_.toString), 0)
    oos.writeObject(setupMsg)
    oos.writeObject(IngestionCommands.UnknownDataset)
    oos.writeObject(BadSchema("no match foo blah"))
    oos.writeObject(Ack(123L))

    val ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray))
    ois.readObject should equal (setupMsg)
    ois.readObject should equal (IngestionCommands.UnknownDataset)
    ois.readObject should equal (BadSchema("no match foo blah"))
    ois.readObject should equal (Ack(123L))
  }

  it("should be able to serialize a ShardMapper") {
    val baos = new ByteArrayOutputStream
    val oos = new ObjectOutputStream(baos)
    val emptyRef = ActorRef.noSender
    val mapper = new ShardMapper(16)
    mapper.registerNode(Seq(4, 7, 8), emptyRef)
    oos.writeObject(mapper)

    val ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray))
    ois.readObject should equal (mapper)
  }

  it("should be able to serialize and deserialize IngestRows with BinaryRecords") {
    import filodb.core.NamesTestData._
    import Serializer._

    putPartitionSchema(projection.partKeyBinSchema)
    putDataSchema(projection.binSchema)
    val records = mapper(names).zipWithIndex.map { case (r, idx) =>
      val record = IngestRecord(projection, r, idx)
      record.copy(partition = projection.partKey(record.partition),
                  data = BinaryRecord(projection.binSchema, record.data))
    }
    val cmd = IngestRows(datasetRef, 0, 1, records)
    fromBinaryIngestRows(cmd.toBytes()) should equal (cmd)
  }

  import filodb.core.query._

  it("should be able to serialize different Aggregates") {
    val baos = new ByteArrayOutputStream
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(DoubleAggregate(99.9))
    val arrayAgg = new ArrayAggregate(10, 0.0)
    oos.writeObject(arrayAgg)
    val pointAgg = new PrimitiveSimpleAggregate(DoubleSeriesValues("foo",
                                                                   Seq(DoubleSeriesPoint(100000L, 1.2))))
    oos.writeObject(pointAgg)

    val ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray))
    ois.readObject should equal (DoubleAggregate(99.9))
    val deserArrayAgg = ois.readObject.asInstanceOf[ArrayAggregate[Double]]
    deserArrayAgg.result should === (arrayAgg.result)
    val deserPointAgg = ois.readObject.asInstanceOf[PrimitiveSimpleAggregate[DoubleSeriesValues]]
    deserPointAgg.data should equal (pointAgg.data)
  }
}