package filodb.coordinator

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import akka.actor.ActorRef
import org.scalatest.{FunSpec, Matchers}

import filodb.core.NamesTestData
import filodb.core.binaryrecord.BinaryRecord
import filodb.core.memstore.{IngestRecord, IngestRouting}

class SerializationSpec extends FunSpec with Matchers {
  import IngestionCommands._
  import NodeClusterActor._
  import NamesTestData._

  it("should be able to serialize different IngestionCommands messages") {
    val baos = new ByteArrayOutputStream
    val oos = new ObjectOutputStream(baos)
    val setupMsg = DatasetSetup(dataset.asCompactString)
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
    import Serializer._
    import filodb.core.NamesTestData._

    putPartitionSchema(dataset.partitionBinSchema)
    putDataSchema(dataset.dataBinSchema)
    val routing = IngestRouting(dataset, Seq("first", "last", "age", "seg"))
    val records = mapper(names).zipWithIndex.map { case (r, idx) =>
      val record = IngestRecord(routing, r, idx)
      record.copy(partition = dataset.partKey(record.partition),
                  data = BinaryRecord(dataset.dataBinSchema, record.data))
    }
    val cmd = IngestRows(dataset.ref, 1, records)
    fromBinaryIngestRows(cmd.toBytes()) should equal (cmd)
  }

  import filodb.core.query._

  it("should be able to serialize different Aggregates") {
    val baos = new ByteArrayOutputStream
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(DoubleAggregate(99.9))
    val arrayAgg = new ArrayAggregate(10, 0.0)
    oos.writeObject(arrayAgg)
    val pointAgg = new PrimitiveSimpleAggregate(DoubleSeriesValues(3, "foo",
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