package filodb.core.metadata

import filodb.core._
import filodb.core.SingleKeyTypes._
import org.velvia.filo.RowReader

object ComputedKeyTypes {
  def getComputedType(keyType: KeyType)(func: RowReader => keyType.T): KeyType = keyType match {
    case kt: IntKeyTypeLike    => new ComputedIntKeyType(func.asInstanceOf[RowReader => Int])
    case kt: LongKeyTypeLike   => new ComputedLongKeyType(func.asInstanceOf[RowReader => Long])
    case kt: DoubleKeyTypeLike => new ComputedDoubleKeyType(func.asInstanceOf[RowReader => Double])
    case kt: StringKeyTypeLike => new ComputedStringKeyType(func.asInstanceOf[RowReader => String])
  }

  class ComputedIntKeyType(func: RowReader => Int) extends IntKeyTypeLike {
    override def getKeyFunc(columnNumbers: Array[Int]): RowReader => Int = func
  }

  class ComputedLongKeyType(func: RowReader => Long) extends LongKeyTypeLike {
    override def getKeyFunc(columnNumbers: Array[Int]): RowReader => Long = func
  }

  class ComputedDoubleKeyType(func: RowReader => Double) extends DoubleKeyTypeLike {
    override def getKeyFunc(columnNumbers: Array[Int]): RowReader => Double = func
  }

  class ComputedStringKeyType(func: RowReader => String) extends StringKeyTypeLike {
    override def getKeyFunc(columnNumbers: Array[Int]): RowReader => String = func
  }
}