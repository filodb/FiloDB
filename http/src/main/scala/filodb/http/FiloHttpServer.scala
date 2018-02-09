package filodb.http

import scala.concurrent.Await

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import com.typesafe.scalalogging.StrictLogging

trait FiloRoute {
  def route: Route
}

class FiloHttpServer(actorSystem: ActorSystem) extends StrictLogging {

  val settings = new HttpSettings(actorSystem.settings.config)

  /**
    * Starts the HTTP Server, blocks until it is up.
    *
    * @param coordinatorRef the ActorRef to the local NodeCoordinator
    * @param clusterProxy the ClusterSingletonProxy ActorRef to the NodeClusterActor singleton
    * @param externalRoutes Additional routes to add besides those configured within the module
    */
  def start(coordinatorRef: ActorRef,
            clusterProxy: ActorRef,
            externalRoutes: Route = reject): Unit = {
    // This is a preliminary implementation of routes. Will be enhanced later
    val filoRoutes: List[FiloRoute] = List(AdminRoutes,
                                           new ClusterApiRoute(clusterProxy),
                                           HealthRoute)
    val reduced = filoRoutes.foldLeft[Route](reject)((acc, r) => r.route ~ acc)
    implicit val system = actorSystem
    implicit val materializer = ActorMaterializer()
    val finalRoute = reduced ~ externalRoutes
    val bindingFuture = Http().bindAndHandle(finalRoute,
      settings.httpServerBindHost,
      settings.httpServerBindPort)
    val bind = Await.result(bindingFuture,
      scala.concurrent.duration.Duration.fromNanos(settings.httpServerStartTimeout.toNanos))
    logger.info("FiloDB HTTP server is live at http:/{}/", bind.localAddress)
  }
}

