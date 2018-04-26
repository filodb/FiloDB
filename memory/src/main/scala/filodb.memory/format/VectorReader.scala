package filodb.memory.format

import java.nio.ByteBuffer

import filodb.memory.format.vectors._

case class UnsupportedFiloType(vectType: Int, subType: Int) extends
  Exception(s"Unsupported Filo vector type $vectType, subType $subType")

/**
 * VectorReader is a type class to help create FiloVector's from the raw Filo binary byte buffers --
 * mostly parsing the header bytes and ensuring the creation of the right FiloVector parsing class.
 *
 * NOTE: I KNOW there is LOTS of repetition here, but apply() method is the inner loop and must be
 * super fast.  Traits would slow it WAY down.  Instead maybe we can use macros.
 */
object VectorReader {
  import TypedBufferReader._
  import WireFormat._
  val forImportingImplicit = FOR_IMPLICIT

  implicit object BoolVectorReader extends PrimitiveVectorReader[Boolean]

  implicit object IntVectorReader extends PrimitiveVectorReader[Int] {

    override val otherMaker: PartialFunction[(Int, Int, ByteBuffer), FiloVector[Int]] = {
      case (VECTORTYPE_BINSIMPLE, SUBTYPE_INT, b)        => IntBinaryVector.masked(b)
      case (VECTORTYPE_BINSIMPLE, SUBTYPE_INT_NOMASK, b) => vectors.IntBinaryVector(b)
      case (VECTORTYPE_BINSIMPLE, SUBTYPE_REPEATED, b)   => vectors.IntBinaryVector.const(b)
    }
  }

  implicit object LongVectorReader extends PrimitiveVectorReader[Long] {

    override val otherMaker: PartialFunction[(Int, Int, ByteBuffer), FiloVector[Long]] = {
      case (VECTORTYPE_DELTA2,    SUBTYPE_INT_NOMASK, b) => DeltaDeltaVector(b)
      case (VECTORTYPE_DELTA2,    SUBTYPE_REPEATED, b)   => DeltaDeltaVector.const(b)
      case (VECTORTYPE_BINSIMPLE, SUBTYPE_PRIMITIVE, b)  => vectors.LongBinaryVector.masked(b)
      case (VECTORTYPE_BINSIMPLE, SUBTYPE_PRIMITIVE_NOMASK, b) => vectors.LongBinaryVector(b)
      // deprecated, not in use anymore
      case (VECTORTYPE_BINSIMPLE, SUBTYPE_INT, b)        => LongBinaryVector.fromMaskedIntBuf(b)
      case (VECTORTYPE_BINSIMPLE, SUBTYPE_INT_NOMASK, b) => vectors.LongBinaryVector.fromIntBuf(b)
    }
  }

  implicit object DoubleVectorReader extends PrimitiveVectorReader[Double] {
    override val otherMaker: PartialFunction[(Int, Int, ByteBuffer), FiloVector[Double]] = {
      case (VECTORTYPE_DELTA2,    SUBTYPE_INT_NOMASK, b) => DoubleVector.fromDDVBuf(b)
      case (VECTORTYPE_DELTA2,    SUBTYPE_REPEATED, b)   => DoubleVector.fromConstDDVBuf(b)
      case (VECTORTYPE_BINSIMPLE, SUBTYPE_REPEATED, b)   => vectors.DoubleVector.const(b)
      case (VECTORTYPE_BINSIMPLE, SUBTYPE_PRIMITIVE, b)  => vectors.DoubleVector.masked(b)
      case (VECTORTYPE_BINSIMPLE, SUBTYPE_PRIMITIVE_NOMASK, b) => vectors.DoubleVector(b)
    }
  }

  implicit object FloatVectorReader extends PrimitiveVectorReader[Float]


  implicit object UTF8VectorReader extends VectorReader[ZeroCopyUTF8String] {
    def makeVector(buf: ByteBuffer, headerBytes: Int): FiloVector[ZeroCopyUTF8String] = {
      (majorVectorType(headerBytes), vectorSubType(headerBytes)) match {
        case (VECTORTYPE_BINSIMPLE, SUBTYPE_UTF8)     => UTF8Vector(buf)
        case (VECTORTYPE_BINSIMPLE, SUBTYPE_FIXEDMAXUTF8) => vectors.UTF8Vector.fixedMax(buf)
        case (VECTORTYPE_BINDICT, SUBTYPE_UTF8)       => DictUTF8Vector(buf)
        case (VECTORTYPE_BINSIMPLE, SUBTYPE_REPEATED) => vectors.UTF8Vector.const(buf)
        case (vectType, subType) => throw UnsupportedFiloType(vectType, subType)
      }
    }
  }

}

/**
 * Implemented by specific Filo column/vector types.
 */
trait VectorReader[A] {
  /**
   * Creates a FiloVector based on the remaining bytes.  Needs to decipher
   * what sort of vector it is and make the appropriate choice.
   * @param buf a ByteBuffer of the binary vector, with the position at right after
   *            the 4 header bytes... at the beginning of FlatBuffers or whatever
   * @param the four byte headerBytes
   */
  def makeVector(buf: ByteBuffer, headerBytes: Int): FiloVector[A]
}

// NOTE: we MUST @specialize here so that the apply method below will not create boxing
class PrimitiveVectorReader[@specialized A: TypedReaderProvider] extends VectorReader[A] {
  import WireFormat._

  def makeVector(buf: ByteBuffer, headerBytes: Int): FiloVector[A] =
    vectMaker((majorVectorType(headerBytes), vectorSubType(headerBytes), buf))


  val defaultMaker: PartialFunction[(Int, Int, ByteBuffer), FiloVector[A]] = {
    case (vectType, subType, _) => throw UnsupportedFiloType(vectType, subType)
  }

  def otherMaker: PartialFunction[(Int, Int, ByteBuffer), FiloVector[A]] = Map.empty

  lazy val vectMaker = otherMaker orElse defaultMaker

}
