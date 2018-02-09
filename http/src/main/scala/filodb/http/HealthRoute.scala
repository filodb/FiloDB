package filodb.http

import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._

object HealthRoute extends FiloRoute {
  val route = {
    path ("__health") {
      get {
        // TODO change this to make the health more meaningful for FiloDB.
        complete(HttpEntity(ContentTypes.`text/plain(UTF-8)`, "All good."))
      }
    }
  }
}
