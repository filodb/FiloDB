package filodb.query.exec.rangefn

import java.util.regex.PatternSyntaxException

import filodb.core.query.{CustomRangeVectorKeyWithShards, RangeVectorKey}
import filodb.memory.format.ZeroCopyUTF8String
import filodb.query.InstantFunctionId
import filodb.query.InstantFunctionId.LabelReplace

trait LabelTypeInstantFunction {

  /**
    * Apply the required instant function against the given value.
    *
    * @param value RangeVectorKey against which the function will be applied
    * @return Updated RangeVectorKey
    */
  def apply(value: RangeVectorKey): RangeVectorKey

}

object LabelFunction {

  /**
    * This function returns a function that can be applied to generate the result.
    *
    * @param function to be invoked
    * @param funcParams - Additional required function parameters
    * @return the function
    */
  def apply(function: InstantFunctionId, funcParams: Seq[Any]): LabelTypeInstantFunction =
  {
    function match {
      case LabelReplace       => LabelReplaceImpl(funcParams)
      case _                  => throw new UnsupportedOperationException(s"$function not supported.")
    }
  }
}

case class LabelReplaceImpl(funcParams: Seq[Any]) extends LabelTypeInstantFunction {

  /**
    * Validate the function before invoking the function.
    */
  require(funcParams.size == 4,
    "Cannot use LabelReplace without function parameters: " +
      "instant-vector, dst_label string, replacement string, src_label string,regex string")

  override def apply(rangeVectorKey: RangeVectorKey): RangeVectorKey = {
    val dstLabel = funcParams(0).asInstanceOf[String]
    val replacementString = funcParams(1).asInstanceOf[String]
    val srcLabel = funcParams(2).asInstanceOf[String]
    try {
      val regex = funcParams(3).asInstanceOf[String].r
      val value = rangeVectorKey.labelValues.get(ZeroCopyUTF8String(srcLabel))

      if (value.isDefined) {
        val labelReplaceValue = regex.replaceAllIn(value.get.toString, replacementString)
        if (!labelReplaceValue.equals(value.get.toString)) {
          return CustomRangeVectorKeyWithShards(rangeVectorKey.labelValues.
            updated(ZeroCopyUTF8String(dstLabel), ZeroCopyUTF8String(labelReplaceValue)), rangeVectorKey.sourceShards)

        }
      }
    }
    catch {
      case ex: PatternSyntaxException => {
        require(false, "Invalid Regular Expression")
        ex.printStackTrace()
      }
    }

    return rangeVectorKey;

  }

}
