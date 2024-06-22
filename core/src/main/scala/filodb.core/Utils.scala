package filodb.core

import java.lang.management.ManagementFactory
import com.typesafe.config.{Config, ConfigRenderOptions}
import com.typesafe.scalalogging.StrictLogging
import filodb.core.query.{ColumnFilter, Filter}


import scala.collection.mutable.ArrayBuffer
import scala.collection.{Seq, mutable}

object Utils extends StrictLogging {
  private val threadMbean = ManagementFactory.getThreadMXBean
  private val cpuTimeEnabled = threadMbean.isCurrentThreadCpuTimeSupported && threadMbean.isThreadCpuTimeEnabled
  logger.info(s"Measurement of CPU Time Enabled: $cpuTimeEnabled")

  def currentThreadCpuTimeNanos: Long = {
    if (cpuTimeEnabled) threadMbean.getCurrentThreadCpuTime
    else System.nanoTime()
  }

  def calculateAvailableOffHeapMemory(filodbConfig: Config): Long = {
    val containerMemory = ManagementFactory.getOperatingSystemMXBean()
      .asInstanceOf[com.sun.management.OperatingSystemMXBean].getTotalPhysicalMemorySize()
    val currentJavaHeapMemory = Runtime.getRuntime().maxMemory()
    val osMemoryNeeds = filodbConfig.getMemorySize("memstore.memory-alloc.os-memory-needs").toBytes
    logger.info(s"Detected available memory containerMemory=$containerMemory" +
      s" currentJavaHeapMemory=$currentJavaHeapMemory osMemoryNeeds=$osMemoryNeeds")

    logger.info(s"Memory Alloc Options: " +
      s"${filodbConfig.getConfig("memstore.memory-alloc").root().render(ConfigRenderOptions.concise())}")

    val availableMem = if (filodbConfig.hasPath("memstore.memory-alloc.available-memory-bytes")) {
      val avail = filodbConfig.getMemorySize("memstore.memory-alloc.available-memory-bytes").toBytes
      logger.info(s"Using automatic-memory-config using overridden memory-alloc.available-memory $avail")
      avail
    } else {
      logger.info(s"Using automatic-memory-config using without available memory override")
      containerMemory - currentJavaHeapMemory - osMemoryNeeds
    }
    logger.info(s"Available memory calculated or configured as $availableMem")
    availableMem
  }

  /**
   * Simple HashMap-node trie implementation.
   */
  class Trie[K, V] {
    private class Node(val next: mutable.Map[K, Node],
                       var element: Option[V]) {
      /**
       * Prints the Node tree; useful for debugging.
       * @param key the key used to access this node (if it exists).
       */
      def printTree(indent: Int = 0, key: Option[K] = None): String = {
        val indentStr = "-".repeat(indent)
        val elementStr = element.toString().replaceAll("\\n", "\\n")
        val keyStr = key.map(k => s"${k.toString}: ")
          .getOrElse("")
          .replaceAll("\\n", "\\n")
        val rootStr = s"$indentStr$keyStr$elementStr"
        if (next.isEmpty) {
          return rootStr
        }
        val childrenStr = next.map{case (k, node) =>
          node.printTree(indent + 1, Some(k))
        }.mkString("\n")
        s"$rootStr\n$childrenStr"
      }
    }

    private val root = new Node(
      next = new mutable.HashMap[K, Node],
      element = None)

    /**
     * Adds an element to the trie at the argument path.
     * @param path must not already be occupied by an element.
     */
    def add(path: Seq[K], element: V): Unit = {
      var ptr = root
      for (key <- path) {
        val node = new Node(
          next = new mutable.HashMap[K, Node],
          element = None)
        ptr = ptr.next.getOrElseUpdate(key, node)
      }
      if (ptr.element.isDefined) {
        throw new IllegalArgumentException(
          s"element already exists at path; exists=${ptr.element.get}; new=$element")
      }
      ptr.element = Some(element)
    }

    /**
     * Returns an occupied Option iff the path is occupied by an element.
     */
    def get(path: Seq[K]): Option[V] = {
      var ptr = root
      for (key <- path) {
        if (!ptr.next.contains(key)) {
          return None
        }
        ptr = ptr.next(key)
      }
      ptr.element
    }

    /**
     * Prints the trie; useful for debugging.
     */
    def printTree(): String = root.printTree()
  }

  /**
   * Maps sets of {@link ColumnFilter} "keys" to element "values".
   *
   * Convenient for use-cases that require e.g. configuration of rules against
   *   label-value filters (filter->rule pairs). The {@link #get} method
   *   accepts a set of label-value pairs, identifies any matching filter sets,
   *   then returns the corresponding element.
   *
   * For example, consider the (filters,element) pairs:
   *     - ({l1="v1",l2=~"v2.*"}, elt1)
   *     - ({l1="v1",l2="v3"},    elt2)
   *     - ({l2=~"v4.*"},         elt3)
   *
   * The following {label:value} sets would map to these elements:
   *     - {l1=v1,l2=v2,l3=v3} -> elt1
   *     - {l1=v1}             -> (None)
   *     - {l2=v4A}            -> elt3
   *
   * A label-value map "matches" a set of filters when:
   *     - all filter labels are present in the map
   *     - every filter is matched by label in the map.
   *
   * NOTE: at most one {@link ColumnFilter} in a set can be non-Equals.
   *
   * NOTE: This implementation is intended to favor simplicity over efficiency.
   *   Other implementations should be considered if the {@link #get} runtime
   *   (described in the method's javadoc) is insufficient.
   *
   * @tparam T the type of elements to store/return.
   * @param filterElementPairs (filter-set,element) pairs; at most one of filter-set
   *                           can be a non-Equals filter.
   */
  class ColumnFilterMap[T](filterElementPairs: Iterable[(Iterable[ColumnFilter], T)]){

    // The main idea:
    // - Require that filter sets added map "keys" include at most one non-Equals filter.
    // - Use tries to match get() argument label-values pairs with Equals filters efficiently
    //   (see get() for more details).
    // - The trie get() matches Equals filters; store each element with its one final
    //   (possibly non-Equals) filter as values in the trie. Iterate these filters as appropriate.

    /**
     * Maps individual filters to elements.
     * Equals filters are stored as a Map for efficiency.
     */
    protected case class FilterToElementMap(equalsFilters: Map[String, T],
                                            otherFilters: Iterable[(ColumnFilter, T)]) {
      /**
       * Returns an occupied Option iff at least one filter is matched.
       * The Equals map is checked first before other filters are iterated.
       */
      def get(string: String): Option[T] = {
        if (equalsFilters.contains(string)) {
          return Some(equalsFilters(string))
        }
        val matchingFilter = otherFilters.find{ case (filter, _) => filter.filter.filterFunc(string) }
        if (matchingFilter.isDefined) {
          val (_, elt) = matchingFilter.get
          return Some(elt)
        }
        None  // No match found.
      }
    }

    // Stores all the tries (for matching Equals filters).
    // Keys are found deterministically from each set of ColumnFilters to store.
    val labelSeqToTrie = new mutable.HashMap[Seq[String], Trie[String, FilterToElementMap]]

    filterElementPairs.foreach{ case (filters, _) =>
      require(filters.count(!_.filter.isInstanceOf[Filter.Equals]) <= 1,
        "at most one filter can be non-Equals; filters=" + filters)
    }

    filterElementPairs.map{ case (filters, elt) =>
      // Sort the filters to determine the appropriate labelSeqToTrie key.
      // Sort alphabetically, then move any non-Equals filters to the end.
      val sortedFilters = filters.toSeq
        .sortBy(_.column)
        .sortWith { case (leftFilter, rightFilter) =>
          leftFilter.filter.isInstanceOf[Filter.Equals] &&
            !rightFilter.filter.isInstanceOf[Filter.Equals]
        }
        (sortedFilters, elt)
    }.groupBy{ case (sortedFilters, _) =>
      // Group each set of filters by:
      //   - its sequence of label names (determines the trie it will be assigned to)
      //   - its sequence of label values (excluding the last value; this "value prefix"
      //     determines the element's storage path in the trie).
      val labelNames = sortedFilters.map(_.column)
      val prefixValues = sortedFilters
        .dropRight(1)
        .map(_.filter.asInstanceOf[Filter.Equals].value.toString)
      (labelNames, prefixValues)
    }.foreach{ case ((labelNames, prefixValues), sortedFiltersToElements) =>
      // All (filters,element) pairs here will be grouped at the same path in the same trie.
      // For some efficiency, we store all Equals filters as part of a map. All non-Equals
      //   filters are stored in a separate list (to be iterated sequentially).
      val equalsFilterMap = new mutable.HashMap[String, T]
      val otherFilters = new ArrayBuffer[(ColumnFilter, T)]
      sortedFiltersToElements.foreach { case (filters, element) =>
        filters.last.filter match {
          case Filter.Equals(value) => equalsFilterMap(value.toString) = element
          case _ => otherFilters.append((filters.last, element))
        }
      }
      val filterToElementMap = FilterToElementMap(equalsFilterMap.toMap, otherFilters)
      labelSeqToTrie
        .getOrElseUpdate(labelNames, new Trie())
        .add(prefixValues, filterToElementMap)
    }

    /**
     * Returns an occupied {@link Option} iff the labels matched an
     *   entire set of {@link ColumnFilter}s.
     * An element is chosen arbitrarily when multiple sets are matched.
     *
     * Runtime grows (among other ways):
     *     - linearly with the max count of filters in a key set.
     *     - linearly with the count of distinct sets of key filter label names.
     *   This data-structure should only be used where each of these is reasonably small.
     */
    def get(labels: collection.Map[String, String]): Option[T] = {
      // The tries we've built require that certain labels are present in the set.
      // Identify the set of (trie, labels-names) pairs s.t. all labels are present,
      //   then map these to the set of (trie, label-values) pairs for efficiency.
      val trieValuesPairs = labelSeqToTrie
        .map{ case (filterLabels, trie) => (trie, filterLabels.map(labels.get)) }  // Get all value options.
        .filter { case (_, filterValues) => filterValues.forall(_.isDefined) }     // Make sure all are defined.
        .map { case (trie, filterValues) => (trie, filterValues.map(_.get)) }      // Extract the values.
      for ((trie, values) <- trieValuesPairs) {
        // Get the set of final ColumnFilters from the trie, then
        //   check each against the final label value.
        val valuesPrefix = values.dropRight(1)
        val matchedElement = trie.get(valuesPrefix)
          .flatMap(filterToElementMap => filterToElementMap.get(values.last))
        if (matchedElement.isDefined) {
          return matchedElement
        }
      }
      None  // No matches were found.
    }

    /**
     * Prints the trie; useful for debugging.
     */
    def printTree(): String = {
      labelSeqToTrie.map{ case (labels, trie) =>
        s"######## labels=$labels\n${trie.printTree()}"
      }.mkString("\n")
    }
  }
}

object Foo {

  def makeEquals(label: String, value: String): ColumnFilter = {
    ColumnFilter(label, filodb.core.query.Filter.Equals(value))
  }

  def makeRegex(label: String, value: String): ColumnFilter = {
    ColumnFilter(label, filodb.core.query.Filter.EqualsRegex(value))
  }
  def main(args: Array[String]): Unit = {

    val elements = Seq(
      // some config
      ("a", Seq(makeEquals("_ws_", "v1"), makeEquals("_ns_", "v2"))),
      // some other config
      ("b", Seq(makeEquals("_ws_", "v1"), makeEquals("_ns_", "v3"))),
      // some single-label config
      ("c", Seq(makeEquals("_ws_", "v4"))),
      // some single-label regex config
      ("d", Seq(makeRegex("_ws_", "v5.*"))),
      // another single-label regex config
      ("e", Seq(makeEquals("_ns_", "v6.*"))),
      // three-label config
      ("a", Seq(makeEquals("_ws_", "A"), makeEquals("_ns_", "B"), makeRegex("_metric_", "C"))),
      ("a", Seq(makeEquals("_ws_", "D"), makeEquals("_ns_", "B"), makeRegex("_metric_", "C"))),
    )
    val trie = new Utils.ColumnFilterMap[String](elements.map{ case (a, b) => (b, a)})

    println(trie.printTree())
    println(trie.get(Map("_ws_" -> "v4")))

  }
}
