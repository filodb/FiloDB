package filodb.memory.format

trait TypedBufferReader[@specialized A] {
  def read(i: Int): A
}

trait TypedReaderProvider[A] {
  def getReader(reader: FastBufferReader): TypedBufferReader.NBitsToReader[A]
}

object TypedBufferReader {
  final val FOR_IMPLICIT = 0

  type NBitsToReader[A] = PartialFunction[(Int, Boolean), TypedBufferReader[A]]

  // scalastyle:off
  def UnsupportedReaderPF[A]: NBitsToReader[A] = {
    case (nbits, signed) => throw new RuntimeException(s"Unsupported Filo vector nbits=$nbits signed=$signed")
  }
  // scalastyle:on

  def apply[A: TypedReaderProvider](reader: FastBufferReader, nbits: Int, signed: Boolean):
    TypedBufferReader[A] =
      (implicitly[TypedReaderProvider[A]].getReader(reader) orElse UnsupportedReaderPF)((nbits, signed))

  implicit object BoolReaderProvider extends TypedReaderProvider[Boolean] {
    def getReader(reader: FastBufferReader): NBitsToReader[Boolean] = {
      case (64, _) =>
        new TypedBufferReader[Boolean] {
          final def read(i: Int): Boolean = ((reader.readByte(i >> 3) >> (i & 0x07)) & 0x01) != 0
        }
    }
  }

  implicit object IntReaderProvider extends TypedReaderProvider[Int] {
    def getReader(reader: FastBufferReader): NBitsToReader[Int] = {
      case (32, _)     => new TypedBufferReader[Int] {
                            final def read(i: Int): Int = reader.readInt(i)
                          }
      case (16, true)  => new TypedBufferReader[Int] {
                            final def read(i: Int): Int = reader.readShort(i).toInt
                          }
      case (8, true)   => new TypedBufferReader[Int] {
                            final def read(i: Int): Int = reader.readByte(i).toInt
                          }
      case (16, false) => new TypedBufferReader[Int] {
                            final def read(i: Int): Int = (reader.readShort(i) & 0x0ffff).toInt
                          }
      case (8, false)  => new TypedBufferReader[Int] {
                            final def read(i: Int): Int = (reader.readByte(i) & 0x00ff).toInt
                          }
    }
  }

  implicit object LongReaderProvider extends TypedReaderProvider[Long] {
    def getReader(reader: FastBufferReader): NBitsToReader[Long] = {
      case (64, _)     => new TypedBufferReader[Long] {
                            final def read(i: Int): Long = reader.readLong(i)
                          }
      case (32, true)  => new TypedBufferReader[Long] {
                            final def read(i: Int): Long = reader.readInt(i).toLong
                          }
      case (16, true)  => new TypedBufferReader[Long] {
                            final def read(i: Int): Long = reader.readShort(i).toLong
                          }
      case (8, true)   => new TypedBufferReader[Long] {
                            final def read(i: Int): Long = reader.readByte(i).toLong
                          }
      case (32, false) => new TypedBufferReader[Long] {
                            final def read(i: Int): Long = (reader.readInt(i) & 0x0ffffffffL).toLong
                          }
      case (16, false) => new TypedBufferReader[Long] {
                            final def read(i: Int): Long = (reader.readShort(i) & 0x0ffff).toLong
                          }
      case (8, false)  => new TypedBufferReader[Long] {
                            final def read(i: Int): Long = (reader.readByte(i) & 0x00ff).toLong
                          }
    }
  }

  implicit object DoubleReaderProvider extends TypedReaderProvider[Double] {
    def getReader(reader: FastBufferReader): NBitsToReader[Double] = {
      case (64, false) => new TypedBufferReader[Double] {
                            final def read(i: Int): Double = reader.readDouble(i)
                          }
    }
  }

  implicit object FloatReaderProvider extends TypedReaderProvider[Float] {
    def getReader(reader: FastBufferReader): NBitsToReader[Float] = {
      case (32, false) => new TypedBufferReader[Float] {
                            final def read(i: Int): Float = reader.readFloat(i)
                          }
    }
  }
}