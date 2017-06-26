package filodb.kafka

import org.example._

class SerdeInstanceSpec extends AbstractSpec {

  private val keys = Seq("streams.value.serde", "producer.value.serializer", "consumer.value.deserializer")

  def clearAll(): Unit = keys foreach (System.clearProperty)

  override def beforeAll(): Unit = clearAll()
  override def beforeEach(): Unit = clearAll()
  override def afterAll(): Unit = clearAll()

  "SerDeConverter" must {
    "create streams serde instance from test configured properties" in {
      val streams = new SourceSettings().streamsConfig
      streams.get("value.serde").isDefined must be(true)

      val serde = SerDeInstance.create(streams)
      serde.serializer.getClass must be(classOf[CustomSerializer])
      serde.deserializer.getClass must be(classOf[CustomDeserializer])
    }
    "create custom streams serde from props" in {
      System.setProperty("streams.value.serde", classOf[CustomSerde].getName)
      System.setProperty("producer.value.serializer", classOf[CustomSerializer].getName)
      System.setProperty("consumer.value.deserializer", classOf[CustomDeserializer].getName)
      val streams = new SourceSettings().streamsConfig
      streams("value.serde") must be(classOf[CustomSerde].getName)

      val serde = SerDeInstance.create(streams)
      serde.serializer().getClass must be(classOf[CustomSerializer])
      serde.deserializer().getClass must be(classOf[CustomDeserializer])
    }
  }
}

