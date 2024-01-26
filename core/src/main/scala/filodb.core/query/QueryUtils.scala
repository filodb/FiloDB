package filodb.core.query

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Storage for utility functions.
 */
object QueryUtils {
  val REGEX_CHARS = Array('.', '?', '+', '*', '|', '{', '}', '[', ']', '(', ')', '"', '\\')

  private val regexCharsMinusPipe = (REGEX_CHARS.toSet - '|').toArray

  /**
   * Returns true iff the argument string contains any special regex chars.
   */
  def containsRegexChars(str: String): Boolean = {
    str.exists(REGEX_CHARS.contains(_))
  }

  /**
   * Returns true iff the argument string contains no special regex
   *   characters except the pipe character ('|').
   * True is also returned when the string contains no pipes.
   */
  def containsPipeOnlyRegex(str: String): Boolean = {
    str.forall(!regexCharsMinusPipe.contains(_))
  }

  /**
   * Returns all possible sets of elements where exactly one element is
   *   chosen from each of the argument sequences.
   *
   * @param choices: all sequences should have at least one element.
   * @return ordered sequences; each sequence is ordered such that the element
   *         at index i is chosen from the ith argument sequence.
   */
  def combinations[T](choices: Seq[Seq[T]]): Seq[Seq[T]] = {
    val running = new mutable.ArraySeq[T](choices.size)
    val result = new mutable.ArrayBuffer[Seq[T]]
    def helper(iChoice: Int): Unit = {
      if (iChoice == choices.size) {
        result.append(Nil ++ running)
        return
      }
      for (choice <- choices(iChoice)) {
        running(iChoice) = choice
        helper(iChoice + 1)
      }
    }
    helper(0)
    result
  }

  /**
   * Splits a string on unescaped pipe characters.
   */
  def splitAtUnescapedPipes(str: String): Seq[String] = {
    // match pipes preceded by any count of backslash pairs and either
    //   some non-backslash character or the beginning of a line.
    val regex = "(?<=(^|[^\\\\]))(\\\\\\\\)*(\\|)".r
    // get pipe indices -- third capture group
    val pipeIndices = regex.findAllMatchIn(str).map(_.start(3)).toSeq

    var offset = 0
    val splits = new ArrayBuffer[String](pipeIndices.size + 1)
    var remaining = str
    for (i <- pipeIndices) {
      // split at the pipe and remove it
      val left = remaining.substring(0, i - offset)
      val right = remaining.substring(i - offset + 1)
      splits.append(left)
      remaining = right
      // count of all characters before the remaining suffix (+1 to account for pipe)
      offset = offset + left.size + 1
    }
    splits.append(remaining)
    splits
  }
}
