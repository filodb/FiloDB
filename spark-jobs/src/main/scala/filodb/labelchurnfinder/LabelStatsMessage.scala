package filodb.labelchurnfinder

import java.time.Instant
import java.util.Base64

import io.circe.{Encoder}
import io.circe.generic.semiauto._
/**
 * Data model for label statistics messages sent to Kafka.
 * Matches the schema expected by Pre-Aggregation Rule Manager.
 *
 * One message contains statistics for ALL labels within a workspace/partition.
 */
case class LabelStatisticsMessage(
  workspaceId: String,
  mosaicPartition: String,
  nsGroup: String,
  jobTimestamp: Instant,
  labels: Seq[LabelStatDto]
)

/**
 * Statistics for a single label.
 * HLL sketches are Base64-encoded for JSON serialization.
 */
case class LabelStatDto(
  labelName: String,
  ats1h: Long,
  ats3d: Long,
  ats7d: Long,
  sketchLabelCard1h: String,  // Base64-encoded HLL sketch
  sketchLabelCard3d: String,  // Base64-encoded HLL sketch
  sketchLabelCard7d: String   // Base64-encoded HLL sketch
)

object LabelStatisticsMessage {
  // Circe encoders for JSON serialization
  implicit val instantEncoder: Encoder[Instant] = Encoder.encodeString.contramap[Instant](_.toString)
  implicit val labelStatDtoEncoder: Encoder[LabelStatDto] = deriveEncoder
  implicit val labelStatsMessageEncoder: Encoder[LabelStatisticsMessage] = deriveEncoder

  def sketchToBase64(sketchBytes: Array[Byte]): String = {
    Base64.getEncoder.encodeToString(sketchBytes)
  }
}
