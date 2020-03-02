package filodb.core

/**
 * Common types for asynchronous command/response
 */

// The parent trait for all commands, esp sent down to I/O data storage actors/futures
trait Command

// The parent trait for all responses from the I/O data storage actors/futures
trait Response

// Parent trait for error messages
trait ErrorResponse extends Response

// Common responses
final case object NoSuchCommand extends ErrorResponse
final case object InconsistentState extends ErrorResponse  // for conditional updates - race condition detected!
final case object TooManyRequests extends ErrorResponse    // Need to retry later when limit dies down
final case object DataDropped extends ErrorResponse

final case object NotFound extends Response
final case object NotApplied extends Response
final case object AlreadyExists extends Response
final case object Success extends Response

// Common exceptions
final case class NotFoundError(what: String) extends Exception(what)
final case class StorageEngineException(t: Throwable) extends Exception(t)
final case class MetadataException(t: Throwable) extends Exception(t)
final case class SystemLimitsReachedException(msg: String) extends Exception(msg)
final case class QueryTimeoutException(queryTime: Long, timedOutAt: String) extends
  Exception (s"Query timeout in $timedOutAt after ${queryTime/1000} seconds")