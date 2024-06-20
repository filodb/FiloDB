package filodb.core

import filodb.core.query.ColumnFilter
import com.typesafe.scalalogging.StrictLogging


import scala.collection.mutable

trait TargetSchemaProvider {
  def targetSchemaFunc(filter: Seq[ColumnFilter]): Seq[TargetSchemaChange]
}

final case class StaticTargetSchemaProvider(targetSchemaOpt: Option[Seq[String]] = None) extends TargetSchemaProvider {
  def targetSchemaFunc(filter: Seq[ColumnFilter]): Seq[TargetSchemaChange] = {
    targetSchemaOpt.map(tschema => Seq(TargetSchemaChange(0, tschema))).getOrElse(Nil)
  }
}

final case class TargetSchemaChange(time: Long = 0L, schema: Seq[String])

object TargetSchemaUtils extends StrictLogging {

  /**
   * Used to build the default target-schema provider's search tree.
   */
  private trait TargetSchemaNode {
    /**
     * Returns an occupied Option iff the argument labels match a set of target-schema filters.
     */
    def getTargetSchema(labels: Map[String, String]): Option[Seq[TargetSchemaChange]]

    /**
     * Prints the target-schema search tree.
     * Useful for debugging.
     */
    def printTree(indent: Int = 0): String

    /**
     * Prints the length of the longest path in the target-schema search tree.
     * Useful for debugging.
     */
    def longestPath(): Int
  }

  /**
   * Non-leaf node in the default target-schema provider's backing search tree.
   * @param label the name of the label to use to determine which side of the tree contains candidate target-schemas.
   *              If this label is not present, candidate target-schemas exist only in the left child.
   * @param ltValue label-values less than this have candidate target-schemas only in the left child.
   */
  private class TargetSchemaNonLeafNode(label: String,
                                        ltValue: String,
                                        left: TargetSchemaNode,
                                        right: TargetSchemaNode) extends TargetSchemaNode {
    override def getTargetSchema(labels: Map[String, String]): Option[Seq[TargetSchemaChange]] = {
      val value = labels.get(label)
      if (value.isEmpty || value.get < ltValue) left.getTargetSchema(labels) else right.getTargetSchema(labels)
    }

    override def printTree(indent: Int = 0): String = {
      val indentLine = "-".repeat(indent)
      val rootStr = s"${indentLine}($label < $ltValue)"
      val leftStr = left.printTree(indent + 1)
      val rightStr = right.printTree(indent + 1)
      Seq(rootStr, leftStr, rightStr).mkString("\n")
    }

    override def longestPath(): Int = {
      1 + math.max(left.longestPath(), right.longestPath())
    }
  }

  /**
   * Leaf node in the default target-schema provider's backing search tree.
   */
  private class TargetSchemaLeafNode(filters: Map[String, String],
                                     schema: Seq[TargetSchemaChange]) extends TargetSchemaNode {
    override def getTargetSchema(labels: Map[String, String]): Option[Seq[TargetSchemaChange]] = {
      // Make sure all filters match the argument labels.
      val allFilterLabelsMatch = filters.forall { case (name, value) => labels.get(name).contains(value) }
      if (allFilterLabelsMatch) Some(schema) else None
    }

    override def printTree(indent: Int = 0): String = {
      val indentLine = "-".repeat(indent)
      s"${indentLine}${filters}: ${schema}"
    }

    override def longestPath(): Int = 0
  }


  // scalastyle:off method.length
  /**
   * Invoked by {@link #makeDefaultTargetSchemaProvider}.
   * Returns a {@link TargetSchemaNode} tree for the given set of target-schemas.
   * @param schemas nonempty set of (filter-labels, sharding-labels) pairs.
   */
  private def makeDefaultTargetSchemaProviderTree(schemas: Seq[(Map[String, String], Seq[TargetSchemaChange])]):
  TargetSchemaNode = {

    assert(schemas.nonEmpty, "expected nonempty set of argument schemas")

    if (schemas.size == 1) {
      val (filterLabels, shardingLabels) = schemas.head
      return new TargetSchemaLeafNode(filterLabels, shardingLabels)
    }

    // Map all unique label names to a list of label values.
    // We will use these lists to determine which label will give us
    //   the most bang-for-the-buck branch in a search tree.
    val labelToValues = schemas
      .flatMap{ case (filterLabels, _) => filterLabels.keys }
      .distinct
      .map(_ -> mutable.ArrayBuffer[Option[String]]())
      .toMap

    // For every target-schema, record the value of every label in labelToValues.
    // If a label is not present, a None is recorded.
    for ((schemaFilterLabels, _) <- schemas) {
      for ((label, values) <- labelToValues) {
        values.append(schemaFilterLabels.get(label))
      }
    }

    // Now, labelToValues is complete. We have a mapping like this:
    //     labelA -> val2, val3, val1, None
    //     labelB -> val1, val4, val1, val1
    // We can use this info to decide which label would be most efficient
    //   to branch on in our search tree; we will assign each label a "balance" score
    //   (this process is detailed more in the logic below). "0" is a perfect score and
    //   implies the left and right tree will contain the same count of elements.

    // (score, (label-name, label-value)) tuples.
    val balanceScores = mutable.ArrayBuffer[(Int, (String, String))]()
    for ((labelName, labelValues) <- labelToValues) {
      val sortedValues = labelValues.sorted
      val middleValue = sortedValues(sortedValues.size / 2)  // not a "true" median

      // Find the indices of the first and last occurrences of the middle-most value.
      val iMiddleFirstOccurrence = Utils.findFirstIndexSorted(sortedValues, middleValue)
      val iMiddleLastOccurrence = Utils.findLastIndexSorted(sortedValues, middleValue)
      // Now, find the scores for each of:
      //   - This entire range is included in the left tree.
      //   - This entire range is included in the right tree.
      val middleInLeftScore = {
        val sizeLeft = iMiddleLastOccurrence + 1
        val sizeRight = sortedValues.size - sizeLeft
        -Math.abs(sizeLeft - sizeRight)
      }
      val middleInRightScore = {
        val sizeLeft = iMiddleFirstOccurrence
        val sizeRight = sortedValues.size - sizeLeft
        -Math.abs(sizeLeft - sizeRight)
      }

      if (middleInLeftScore > middleInRightScore) {
        // Since the score is better when the entire range of repeat middle-most values is included
        //   in the left tree, we can confidently assume a value exists after the middle-most.
        val nextLabel = sortedValues(iMiddleLastOccurrence + 1)
        balanceScores.append((middleInLeftScore, (labelName, nextLabel.get)))
      } else {
        balanceScores.append((middleInRightScore, (labelName, middleValue.get)))
      }
    }
    val sorted = balanceScores.sortBy{ case (score, _) => score }
    val (_, (bestLabel, bestValue)) = sorted.last
    val left = {
      val leftSchemas = schemas.filter{ case (filterLabels, _) =>
        !filterLabels.contains(bestLabel) || filterLabels(bestLabel) < bestValue
      }
      makeDefaultTargetSchemaProviderTree(leftSchemas)
    }
    val right = {
      // Same predicate as above, but "filterNot"
      val rightSchemas = schemas.filterNot { case (filterLabels, _) =>
        !filterLabels.contains(bestLabel) || filterLabels(bestLabel) < bestValue
      }
      makeDefaultTargetSchemaProviderTree(rightSchemas)
    }
    new TargetSchemaNonLeafNode(bestLabel, bestValue, left, right)
  }
  // scalastyle:on method.length

  /**
   * Creates a target-schema provider backed by a search tree; each left/right
   *   step is based on the value of a single label.
   * The tree height grows:
   *   - logarithmically with the count of schemas.
   *   - linearly with the count of filter labels used across all schemas.
   * Therefore, this implementation works best when all schemas are filtered
   *   against more-or-less the same set of labels.
   * @param schemas nonempty set of (filter-labels, sharding-labels) pairs.
   */
  def makeDefaultTargetSchemaProvider(schemas: Seq[(Map[String, String], Seq[TargetSchemaChange])]):
  TargetSchemaProvider = {
    val rootNode = makeDefaultTargetSchemaProviderTree(schemas)
    new TargetSchemaProvider() {
      override def targetSchemaFunc(filters: Seq[ColumnFilter]): Seq[TargetSchemaChange] = {
        val equalMap = filters
          .filter(_.filter.isInstanceOf[filodb.core.query.Filter.Equals])
          .map(f => f.column -> f.filter.valuesStrings.head.toString)
          .toMap
        // TODO(a_theimer): make sure this works with unit tests, then remove println below.
        logger.info(s"Created default target-schema provider. " +
          s"Leaves:${schemas.size}; " +
          s"Longest path:${rootNode.longestPath()}; " +
          s"Search tree:\n${rootNode.printTree()}")
        println(s"Created default target-schema provider. " +
          s"Leaves:${schemas.size}; " +
          s"Longest path:${rootNode.longestPath()}; " +
          s"Search tree:\n${rootNode.printTree()}")
        rootNode.getTargetSchema(equalMap).getOrElse(Nil)
      }
    }
  }
}



// TODO(a_theimer): delete this
object RandomTest {
  def makeFilter(k: String, v: String): ColumnFilter = {
    ColumnFilter(k, filodb.core.query.Filter.Equals(v))
  }
  val rand = scala.util.Random
  val labels = Seq("a", "b", "c", "d", "e")
  def main(args: Array[String]): Unit = {
    val schemas = new mutable.ArrayBuffer[(Map[String, String], Seq[TargetSchemaChange])]
    for (ischema <- 0 until 100) {
      val nLabels = 1 + rand.nextInt(labels.size)
      val shuffled = rand.shuffle(labels)
      val myLabels = (0 until nLabels).map(i => shuffled(i) -> rand.nextInt().toString).toMap
      val mySchema = Seq(TargetSchemaChange(time = 0, schema = Seq(ischema.toString)))
      schemas.append((myLabels, mySchema))
    }
    val prov = TargetSchemaUtils.makeDefaultTargetSchemaProvider(schemas)
    val ts = prov.targetSchemaFunc(Seq(makeFilter("a", "1")))
    println(ts)
  }
}