package filodb.coordinator.queryplanner

/**
 * Storage for miscellaneous utility functions.
 */
object QueryUtils {
  /**
   * Returns all possible sets of elements where exactly one element is
   * chosen from each of the argument sequences.
   *
   * @param choices : all sequences should have at least one element.
   * @return ordered sequences; each sequence is ordered such that the element
   *         at index i is chosen from the ith argument sequence.
   */
  def combinations[T](choices: Seq[Seq[T]]): Seq[Seq[T]] = choices match {
    case Nil => Nil
    case head +: Nil => head.map(Seq(_))
    case head +: tail =>
      val tailCombos = combinations(tail)
      head.flatMap(h => {
        tailCombos.map(Seq(h) ++ _)
      })
  }
}
