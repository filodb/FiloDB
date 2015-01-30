package filodb.core.cassandra

import com.datastax.driver.core.exceptions.DriverException
import com.websudos.phantom.Implicits._
import scala.concurrent.Future

import filodb.core.messages._

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
    }
  }
}