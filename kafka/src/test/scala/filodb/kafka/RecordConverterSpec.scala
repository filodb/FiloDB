package filodb.kafka

import filodb.core.MachineMetricsData
import filodb.core.memstore.IngestRecord

import org.example._

class RecordConverterSpec extends AbstractSpec {
  "RecordConverter" must {
    "work" in {
      val converter = RecordConverter("org.example.CustomRecordConverter")
      converter.isInstanceOf[CustomRecordConverter] must be (true)

      val projection = MachineMetricsData.projection
      val data = MachineMetricsData.multiSeriesData().take(20)

      val records = data.zipWithIndex.map { case (values, offset) =>
         converter.convert(projection, Event(values), offset.toLong)
      }

      records.forall(_.forall(_.isInstanceOf[IngestRecord])) must be (true)// add better test
      records.size must equal(data.size)
    }
  }
}
