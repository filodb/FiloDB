package filodb.query.exec.rangefn

import java.util.regex.{Pattern, PatternSyntaxException}

import akka.japi.Option.Some
import monix.reactive.Observable

import filodb.core.query.{CustomRangeVectorKey, IteratorBackedRangeVector, RangeVector, RangeVectorKey}
import filodb.memory.format.ZeroCopyUTF8String

trait MiscellaneousFunction {
  def validator(): Boolean

  def execute(): Observable[RangeVector]
}

case class LabelReplaceFunction(source: Observable[RangeVector], funcParams: Seq[Any])
  extends MiscellaneousFunction {

  val labelIdentifier: String= "[a-zA-Z_][a-zA-Z0-9_:\\-\\.]*"

  private def pattern(): Option[Pattern] = {
    val regex = funcParams(3).asInstanceOf[String]
    try {
      val pattern: Pattern = Pattern.compile(regex)
      Some(pattern)
    }
    catch {
      case ex: PatternSyntaxException => {
        require(false, "Invalid Regular Expression")
        ex.printStackTrace()
      }
        None
    }
  }

  override def validator(): Boolean = {
    if (funcParams.size != 4) {
      require(false,
        "Cannot use LabelReplace without function parameters: " +
          "instant-vector, dst_label string, replacement string, src_label string, regex string")
      return false
    }
    if (!funcParams(0).asInstanceOf[String].matches(labelIdentifier)) {
      require(false, "Invalid destination label name")
      return false
    }
    if (!pattern.isDefined)
      return false;
    return true
  }

  override def execute(): Observable[RangeVector] = {
    source.map { rv =>
      val newLabel = labelReplaceImpl(rv.key, funcParams)
      IteratorBackedRangeVector(newLabel, rv.rows)
    }
  }

  def labelReplaceImpl(rangeVectorKey: RangeVectorKey, funcParams: Seq[Any]): RangeVectorKey = {
    val dstLabel = funcParams(0).asInstanceOf[String]
    val replacementString = funcParams(1).asInstanceOf[String]
    val srcLabel = funcParams(2).asInstanceOf[String]

    val value: ZeroCopyUTF8String = if (rangeVectorKey.labelValues.contains(ZeroCopyUTF8String(srcLabel))) {
      rangeVectorKey.labelValues.get(ZeroCopyUTF8String(srcLabel)).get
    }
    else {
      // Assign dummy value as label_replace should overwrite destination label if the source label is empty but matched
      ZeroCopyUTF8String("")
    }

    val matcher = pattern.get.matcher(value.toString)
    if (matcher.matches()) {
      var labelReplaceValue = replacementString
      for (index <- 1 to matcher.groupCount()) {
        labelReplaceValue = labelReplaceValue.replace(s"$$$index", matcher.group(index))
      }
      labelReplaceValue = labelReplaceValue.replaceAll("\\$[A-Za-z0-9]+", "")
      if (labelReplaceValue.length > 0) {
        return CustomRangeVectorKey(rangeVectorKey.labelValues.
          updated(ZeroCopyUTF8String(dstLabel), ZeroCopyUTF8String(labelReplaceValue)), rangeVectorKey.sourceShards)
      }
      else {
        // Drop label if new value is empty
        return CustomRangeVectorKey(rangeVectorKey.labelValues -
          ZeroCopyUTF8String(dstLabel), rangeVectorKey.sourceShards)
      }
    }

    return rangeVectorKey;
  }
}
