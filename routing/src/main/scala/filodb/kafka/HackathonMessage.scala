package filodb.kafka

import org.apache.kafka.common.serialization.Deserializer

class HackathonDecoder[A] extends Deserializer[A]{

  override def configure(configs: Map[String, _], isKey: Boolean): Unit = {

    ()
  }

  override def deserialize(topic: String, data: Array[Byte]): A = ???
}
