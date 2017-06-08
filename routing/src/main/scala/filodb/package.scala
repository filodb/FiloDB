package filodb

import java.util.{Map => JMap, Properties => JProperties, Collection => JCollection}
import java.util.concurrent.CompletionStage

import scala.concurrent.Future
import scala.collection.JavaConverters._

import scala.concurrent.java8.FuturesConvertersImpl.{CF, P}

package object filodb {

  type FutureT[A] = Future[Either[Throwable, A]]

  /**
    * @param cs the CompletionStage which may eventually supply the completion
    *           for the returned Scala Future
    * @tparam A the type returned from the operation if successful - no timeout
    */
  implicit final class JFutureOps[A](cs: CompletionStage[A]) {

    /** Returns a Scala Future that will be completed with the same value or
      * exception as the given CompletionStage when that completes. Transformations
      * of the returned Future are executed asynchronously as specified by the
      * ExecutionContext that is given to the combinator methods.
      */
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

  implicit final class JavaOps(config: JMap[String, AnyRef]) {

    def asImmutable: Map[String, AnyRef] =
      Map.empty[String, String] ++ config.asScala
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
