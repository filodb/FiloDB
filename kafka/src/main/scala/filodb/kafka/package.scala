package filodb

import java.io.{File => JFile}
import java.util.{Map => JMap, Properties => JProperties}
import java.util.concurrent.{CompletionStage, TimeUnit, Future => JFuture}

import scala.concurrent.{Future, Promise}
import scala.collection.JavaConverters._
import scala.concurrent.java8.FuturesConvertersImpl.CF

/** Simple conversion implicits. */
package object kafka {

  type FutureT[A] = Future[Either[Throwable, A]]

  implicit final class JFutureOps[A](jf: JFuture[A]) {

    def asScala: Future[A] =
      jf match {
        case a: CF[A] => a.wrapped
        case a: CompletionStage[A] => a.asScala
        case _ => //todo
          Promise.successful(jf.get(8000, TimeUnit.MICROSECONDS)).future
      }
  }

  implicit final class AnyOps[A](self: A) {
    def ===(other: A): Boolean = self == other
  }

  implicit final class JavaOps(mutable: JMap[String, Object]) {

    def asImmutable: Map[String, AnyRef] =
      Map.empty[String, AnyRef] ++ mutable.asScala
  }

  implicit final class MapOps(immutable: Map[String, AnyRef]) {

    def asProps: JProperties = {
      val props = new JProperties()
      props.putAll(immutable.asJava)
      props
    }
  }

  implicit final class PropertiesOps(props: JProperties) {

    def asMap: Map[String, AnyRef] = props.asScala.toMap[String, AnyRef]
  }

  implicit final class FileOps(file: JFile) {

    def asMap = {
      val source = scala.io.Source.fromFile(file.getAbsolutePath)
      try {
        val props = new java.util.Properties()
        props.load(source.reader)
        props.asMap
      } finally source.close()
    }
  }

  import scala.language.implicitConversions
  import org.apache.kafka.streams.kstream.Reducer
  import org.apache.kafka.streams.KeyValue

  implicit def BinaryFunctionToReducer[V](f: ((V, V) => V)): Reducer[V] = (l: V, r: V) => f(l, r)

  implicit def Tuple2AsKeyValue[K, V](tuple: (K, V)): KeyValue[K, V] = new KeyValue(tuple._1, tuple._2)

}
