package filodb.core.metadata

import filodb.core._
import filodb.core.SingleKeyTypes._
import org.velvia.filo.RowReader

object ComputedKeyTypes {
  class ComputedIntKeyType(func: RowReader => Int) extends IntKeyTypeLike {
    override def getKeyFunc(columnNumbers: Array[Int]): RowReader => Int = func
  }

  class ComputedStringKeyType(func: RowReader => String) extends StringKeyTypeLike {
    override def getKeyFunc(columnNumbers: Array[Int]): RowReader => String = func
  }
}