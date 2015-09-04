package filodb.core.messages

/**
 * Common messages for inter-Actor communication
 */

// The parent trait for all commands, esp sent down to I/O data storage actors/futures
trait Command

// The parent trait for all responses from the I/O data storage actors/futures
trait Response

// Parent trait for error messages
trait ErrorResponse extends Response

// Common responses
case class StorageEngineException(t: Throwable) extends ErrorResponse
case class MetadataException(t: Throwable) extends ErrorResponse
case object NoSuchCommand extends ErrorResponse
case object InconsistentState extends ErrorResponse  // for conditional updates - race condition detected!
case object TooManyRequests extends ErrorResponse    // Need to retry later when limit dies down

case object NotFound extends Response
case object NotApplied extends Response
case object AlreadyExists extends Response
case object Success extends Response

case class NotFoundError(what: String) extends Exception
