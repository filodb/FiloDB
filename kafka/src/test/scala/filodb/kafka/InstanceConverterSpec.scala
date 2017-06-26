package filodb.kafka

import org.example._

/** See IT tests for more converter tests. */
class InstanceConverterSpec extends AbstractSpec {
  "SerDeInstance" must {
    "create instances from FQCNs for ingestion converters" in {
      val converter = RecordConverter("org.example.CustomRecordConverter")
      converter.isInstanceOf[CustomRecordConverter] must be (true)
    }
  }
}
