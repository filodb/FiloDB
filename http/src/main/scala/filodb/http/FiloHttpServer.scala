package filodb.http

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.Await

trait FiloRoute {
  def route: Route
}

class FiloHttpServer(actorSystem: ActorSystem) extends StrictLogging {

  val settings = new HttpSettings(actorSystem.settings.config)

  /**
    * Starts the HTTP Server, blocks until it is up.
    *
    * @param externalRoutes Additional routes to add besides those configured within the module
    */
  def start(externalRoutes: Route = reject): Unit = {

    // This is a preliminary implementation of routes. Will be enhanced later
    val filoRoutes: List[FiloRoute] = List(HealthRoute)
    val reduced = filoRoutes.foldLeft[Route](reject)((acc, r) => r.route ~ acc)
    implicit val system = actorSystem
    implicit val materializer = ActorMaterializer()
    val finalRoute = reduced ~ externalRoutes
    val bindingFuture = Http().bindAndHandle(finalRoute,
      settings.httpServerBindHost,
      settings.httpServerBindPort)
    val bind = Await.result(bindingFuture,
      scala.concurrent.duration.Duration.fromNanos(settings.httpServerStartTimeout.toNanos))
    logger.info("FiloDB HTTP server is live. Seeds can be seen at http:/{}/seeds", bind.localAddress)
  }
}

