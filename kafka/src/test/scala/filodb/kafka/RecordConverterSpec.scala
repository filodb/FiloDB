package filodb.kafka

import filodb.core.{AbstractSpec, MachineMetricsData}
import filodb.core.memstore.IngestRecord
import org.example._

class RecordConverterSpec extends AbstractSpec {
  "RecordConverter" must {
    val projection = MachineMetricsData.projection

    "create a no-arg RecordConverter instance and convert user data" in {
      val converter = RecordConverter("org.example.CustomRecordConverter")
      converter.isInstanceOf[CustomRecordConverter] should be (true)

      val data = MachineMetricsData.multiSeriesData().take(20)

      val records = data.zipWithIndex.map { case (values, offset) =>
        converter.convert(projection, Event(values), 0, offset.toLong)
      }

      records.forall(_.forall(_.isInstanceOf[IngestRecord])) should be (true)// TODO add better test
      records.size should equal(data.size)
    }
  }
}
