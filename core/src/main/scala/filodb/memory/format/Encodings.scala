package filodb.memory.format

/**
  * Classes to encode a Builder to a queryable binary Filo format.
  * Methods automatically detect the best encoding method to use, but hints are available
  * to pass to the methods.
  *
  * To extend the encoder for additional base types A, implement a type class BuilderEncoder[A].
  */
object Encodings {

  sealed trait EncodingHint

  case object AutoDetect extends EncodingHint

  case object AutoDetectDispose extends EncodingHint // Dispose of old/existing vector
  case object SimpleEncoding extends EncodingHint

  case object DictionaryEncoding extends EncodingHint

  case object DiffEncoding extends EncodingHint

  final case class AutoDictString(spaceThreshold: Double = 0.6, samplingRate: Double = 0.3) extends EncodingHint

}

