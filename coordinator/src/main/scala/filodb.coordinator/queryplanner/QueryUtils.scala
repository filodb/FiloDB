package filodb.coordinator.queryplanner

import scala.collection.mutable

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
}
