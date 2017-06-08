package filodb

import java.util.{Collection => JCollection, Map => JMap, Properties => JProperties}
import java.util.concurrent.{CompletableFuture, CompletionStage, TimeUnit}

import scala.concurrent.Future
import scala.collection.JavaConverters._
import scala.concurrent.java8.FuturesConvertersImpl.{CF, P}

package object implicits {

  import cats.implicits._

  type FutureT[A] = Future[Either[Throwable, A]]
 
  implicit final class JFutureOps[A](cs: CompletionStage[A]) {

    def asScala: Future[A] = {
      cs match {
        case cf: CF[A] => cf.wrapped
        case _ =>
          val p = new P[A](cs)
          cs whenComplete p
          p.future
      }
    }
  }

  implicit final class AnyOps[A](self: A) {
    def ===(other: A): Boolean = self == other
  }

  implicit final class JavaOps(config: JMap[String, _]) {

    def asImmutable: Map[String, Any] =
      Map.empty[String, Any] ++ config.asScala
  }

  implicit final class MapOps(immutable: Map[String, AnyRef]) {

    def asJMap: JMap[String, Object] = immutable.asJava

    def asJCollection: JCollection[(String, Object)] = immutable.asJavaCollection

    def asProps: JProperties = {
      val props = new JProperties()
      props.putAll(immutable.asJava)
      props
    }
  }

  implicit final class PropertiesOps(props: JProperties) {

    def asMap: Map[String, String] = props.asScala.toMap[String, String]
  }

}
