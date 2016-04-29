package filodb.coordinator

import java.io.{ByteArrayInputStream, ObjectInputStream}
import java.io.{ByteArrayOutputStream, ObjectOutputStream}

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
}