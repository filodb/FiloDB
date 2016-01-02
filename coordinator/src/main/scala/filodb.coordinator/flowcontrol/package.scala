package filodb.coordinator

import scala.concurrent.duration.Duration

package reactive {

sealed trait FlowControlMessage

case class Acknowledged(numProcessed: Int) extends FlowControlMessage

case object StopFlow extends FlowControlMessage

case object StartFlow extends FlowControlMessage

case class Backoff(duration: Duration) extends FlowControlMessage

case class MaxRetriesExceeded(retries: Int) extends FlowControlMessage


}
