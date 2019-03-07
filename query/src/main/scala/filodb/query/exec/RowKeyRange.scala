package filodb.query.exec

import filodb.core.binaryrecord.BinaryRecord

sealed trait RowKeyRange

case class RowKeyInterval(from: BinaryRecord, to: BinaryRecord) extends RowKeyRange
case object AllChunks extends RowKeyRange
case object WriteBuffers extends RowKeyRange
case object InMemoryChunks extends RowKeyRange
case object EncodedChunks extends RowKeyRange

