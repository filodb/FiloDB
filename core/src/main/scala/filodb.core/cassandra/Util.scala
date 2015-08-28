package filodb.core.cassandra

import com.datastax.driver.core.exceptions.DriverException
import com.websudos.phantom.dsl._
import java.nio.ByteBuffer
import scala.concurrent.Future

import filodb.core.messages._

/**
 * Utilities for dealing with Cassandra I/O
 */
object Util {
  implicit class ResultSetToResponse(f: Future[ResultSet]) {
    def toResponse(notAppliedResponse: Response = NotApplied): Future[Response] = {
      f.map { resultSet =>
        if (resultSet.wasApplied) Success else notAppliedResponse
      }.recover {
        case e: DriverException => StorageEngineException(e)
      }
    }
  }

  implicit class HandleErrors(f: Future[Response]) {
    def handleErrors: Future[Response] = f.recover {
      case e: DriverException => StorageEngineException(e)
      // from invalid Enum strings, which should never happen, or some other parsing error
      case e: NoSuchElementException   => MetadataException(e)
      case e: IllegalArgumentException => MetadataException(e)
    }
  }

  // Phantom 1.8.x can't deal with ByteBuffers with non-zero position and/or non-zero arrayOffset.
  // Code in 1.9.x seems totally different.  This is a workaround for now, hopefully the new code
  // will be much more performant.
  def strictBytes(origBuf: ByteBuffer): ByteBuffer = {
    val offset = origBuf.arrayOffset + origBuf.position
    val strictBytes = java.util.Arrays.copyOfRange(origBuf.array, offset, offset + origBuf.remaining)
    ByteBuffer.wrap(strictBytes)
  }
}