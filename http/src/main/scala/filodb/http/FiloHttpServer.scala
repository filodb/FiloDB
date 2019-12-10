package filodb.http

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.FiniteDuration

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.ExceptionHandler
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import com.typesafe.scalalogging.StrictLogging

trait FiloRoute {
  def route: Route
}

class FiloHttpServer(actorSystem: ActorSystem) extends StrictLogging {

  val settings = new HttpSettings(actorSystem.settings.config)

  private var binding: Http.ServerBinding = _

  def filoExceptionHandler: ExceptionHandler =
    ExceptionHandler {
      case ex: Exception =>
        extractUri { uri =>
          logger.error(s"Request to uri=$uri failed", ex)
          val errorString = s"Request to uri=$uri failed with ${ex.getClass.getName} ${ex.getMessage}\n" +
            ex.getStackTrace.map(_.toString).mkString("\n")
          complete(HttpResponse(InternalServerError, entity = errorString))
        }
    }

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
    implicit val system = actorSystem
    implicit val materializer = ActorMaterializer()
    // This is a preliminary implementation of routes. Will be enhanced later
    val filoRoutes: List[FiloRoute] = List(AdminRoutes,
                                           new ClusterApiRoute(clusterProxy),
                                           new HealthRoute(coordinatorRef),
                                           new PrometheusApiRoute(coordinatorRef, settings))
    val reduced = filoRoutes.foldLeft[Route](reject)((acc, r) => r.route ~ acc)
    val finalRoute = handleExceptions(filoExceptionHandler) {
      reduced ~ externalRoutes
    }
    val bindingFuture = Http().bindAndHandle(finalRoute,
      settings.httpServerBindHost,
      settings.httpServerBindPort)
    binding = Await.result(bindingFuture,
      scala.concurrent.duration.Duration.fromNanos(settings.httpServerStartTimeout.toNanos))
    logger.info("FiloDB HTTP server is live at http:/{}/", binding.localAddress)
  }

  def shutdown(hardDeadline: FiniteDuration): Future[Http.HttpTerminated] = {
    logger.info("Shutting down HTTP server")
    binding.terminate(hardDeadline)
  }
}

