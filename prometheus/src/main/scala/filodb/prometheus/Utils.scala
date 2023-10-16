package filodb.prometheus

/**
 * Storage for utility functions.
 */
object Utils {
  /**
   * Returns the set of unescaped special regex chars in the argument string.
   * Special chars are: . ? + * | { } [ ] ( ) " \
   */
  def getUnescapedSpecialRegexChars(str: String): Set[Char] = {
    // Match special chars preceded by any count of backslash pairs and either
    //   some non-backslash character or the beginning of a line.
    val regex = "(?<=(^|[^\\\\]))(\\\\\\\\)*([.?+*|{}\\[\\]()\"\\\\])".r
    regex.findAllMatchIn(str)
      .map(_.group(3)) // get the special char -- third capture group
      .map(_(0)) // convert the string to a char
      .toSet
  }

  /**
   * Returns true iff the argument string contains no unescaped special regex characters.
   * The pipe character ('|') is excluded from the set of special regex characters.
   */
  def isPipeOnlyRegex(str: String): Boolean = {
    getUnescapedSpecialRegexChars(str).diff(Set('|')).isEmpty
  }
}
