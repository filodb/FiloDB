package filodb.memory.format

object DefaultValues {
  val DefaultInt = 0
  val DefaultLong = 0L
  val DefaultFloat = 0.0F
  val DefaultDouble = 0.0
  val DefaultBool = false
  val DefaultString = ""
  val DefaultTimestamp = new java.sql.Timestamp(0L)
  val DefaultDateTime = new org.joda.time.DateTime(0L)
  val DefaultUTF8String = ZeroCopyUTF8String("")
}