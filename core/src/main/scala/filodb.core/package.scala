package filodb

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap
import java.util.concurrent.ConcurrentMap
import scala.language.implicitConversions

package object core {
  type SingleKeyType = SingleKeyTypeBase[_]

  import SingleKeyTypes._

  implicit class RichCMap[K, V](orig: ConcurrentMap[K, V]) {
    import java.util.function.{Function => JFunction}

    implicit def scalaFuncToJavaFunc[T, R](func1: Function[T, R]): JFunction[T, R] =
      new JFunction[T, R] {
        override def apply(t: T): R = func1.apply(t)
      }

    def getOrElseUpdate(k: K, func: K => V): V = orig.computeIfAbsent(k, func)
  }

  /**
   * Google's ConcurrentLinkedHashMap allows one to build an LRU cache.
   * In combination with the above RichCLHM class you get atomic getOrElseUpdates.
   */
  def concurrentCache[K, V](numEntries: Int): ConcurrentLinkedHashMap[K, V] =
    new ConcurrentLinkedHashMap.Builder[K, V].maximumWeightedCapacity(numEntries).build

}