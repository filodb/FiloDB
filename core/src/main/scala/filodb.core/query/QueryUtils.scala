package filodb.core.query

import scala.collection.mutable

/**
 * Storage for miscellaneous utility functions.
 */
object QueryUtils {
  /**
   * Returns all possible sets of elements where exactly one element is
   *   chosen from each of the argument sequences.
   *
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
   * Returns the set of unescaped regex chars in the argument string.
   */
  def getUnescapedSpecialRegexChars(str: String): Set[Char] = {
    val regex = "[^\\\\]*([.?+*|{}\\[\\]()\"])".r
    regex.findAllMatchIn(str)
      .map(_.group(1))  // get the special char (i.e. ignore its preceding char)
      .map(_(0))        // convert the string to a char
      .toSet
  }

  /**
   * Returns true iff the argument string contains no unescaped special regex characters.
   * The pipe character ('|') is excluded from the set of special regex characters.
   */
  def isPipeOnlyRegex(str: String): Boolean = {
    getUnescapedSpecialRegexChars(str).diff(Set('|')).isEmpty
  }

  /**
   * Splits a string on unescaped pipe characters.
   */
  def splitOnPipes(str: String): Seq[String] = {
    str.split("(^|[^\\\\])(\\\\\\\\)*\\|")
  }
}
