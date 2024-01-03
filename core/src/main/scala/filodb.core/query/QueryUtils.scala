package filodb.core.query

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
}
