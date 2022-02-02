package filodb.gateway.conversion

trait RecordValidatorVisitor {
  def visit(rec: InfluxRecord): Boolean
  def visit(rec: PrometheusInputRecord): Boolean
  def visit(rec: MetricTagInputRecord): Boolean
}

object LabelSizeValidatorVisitor {
  val LABEL_VALUE_MAX_SIZE_BYTES = 16384  // 2^14; 2^15 is lucene limit
  val BYTES_PER_CHAR = 2
}

class LabelSizeValidatorVisitor extends RecordValidatorVisitor {
  import LabelSizeValidatorVisitor._

  def validateLabelValueSize(size: Int): Boolean = {
    (size * BYTES_PER_CHAR) < LABEL_VALUE_MAX_SIZE_BYTES
  }

  def visit(rec: InfluxRecord): Boolean = {
    // once set true, cannot be switched back to false
    class TripBool() {
      private var tripped_ = false
      def tripIfTrue(value: Boolean): Unit = {
        if (!tripped_) {
          tripped_ = value
        }
      }
      def get(): Boolean = {
        tripped_
      }
    }
    // mark invalid if any of the label values are oversized
    val isInvalid = new TripBool()
    InfluxProtocolParser.parseKeyValues(rec.bytes, rec.tagDelims, rec.endOfTags, new KVVisitor {
      override def apply(bytes: Array[Byte], keyIndex: Int, keyLen: Int, valueIndex: Int, valueLen: Int): Unit = {
        isInvalid.tripIfTrue(!validateLabelValueSize(valueLen))
      }
    })
    !isInvalid.get()
  }

  def visit(rec: PrometheusInputRecord): Boolean = {
    rec.tags.values.forall(value => validateLabelValueSize(value.size))
  }

  def visit(rec: MetricTagInputRecord): Boolean = {
    rec.tags.values.forall(value => value.numBytes < LABEL_VALUE_MAX_SIZE_BYTES)
  }
}
