package filodb.core.query

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Maps sets of ColumnFilter "keys" to element "values".
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
 * NOTE: at most one ColumnFilter in a set can be non-Equals.
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
  // - Require that filter sets include at most one non-Equals filter.
  // - For each set of filters, deterministically extract a "key" sequence of label names
  //   and map each key to an inner hash-map. Label names are sorted alphabetically, then any
  //   non-Equals filters are moved to the end. For example, the following filter sets
  //   would have the keys:
  //     1) {labelA="value1a", labelB="value2a"} -> (labelA, labelB)
  //     2) {labelA="value1b", labelB="value2b"} -> (labelA, labelB)  // Same as above.
  //     3) {labelC=~"value3", labelD="value4", labelE="value5"} -> (labelD, labelE, labelC)  // Regex at end.
  //     4) {labelC=~"value3", labelD=~"value4", labelE="value5"} -> INVALID! // >1 regex filter.
  // - The inner hash-maps are keyed on a prefix of these values (all except the last), and
  //   the final filter-- along with the element to store-- is stored as the value. For example,
  //   the inner hash-map of example (3) above would be keyed on (labelD-value, labelE-value) and
  //   store (labelC-filter, element) pairs.
  // - When a set of labels-value pairs is processed through the ColumnFilterMap, the values are
  //   used to access each inner map until (a) the inner map contains a corresponding value, and
  //   (b) the final filter matches the final label-value. When a match is found, the filter's paired
  //   element is returned.
  //
  // As an example, consider a ColumnFilterMap composed of these entries:
  //
  //     {l1="v1", l2="v2", l3="v3a"} -> A
  //     {l1="v1", l2="v2", l3="v3b"} -> B
  //     {l1="v1", l2="v2", l4="v4"} -> C
  //     {l1="v1", l2="v2", l3=~"v3.*"} -> D
  //     {l1="v1", l2=~"v2.*", l3="v3"} -> E
  //     {l1="v1", l2="v2a"} -> F
  //     {l1="v1", l2="v2b"} -> G
  //     {l1="v1a", l2="v2a"} -> H
  //     {l1="v1"} -> I
  //
  //   The nested maps would be structured as:
  //
  //     (l1, l2, l3) -> {
  //         (v1, v2) -> [l3="v3a":A, l3="v3b":B, l3=~"v3.*":D]
  //     }
  //     (l1, l2, l4) -> {
  //         (v1, v2) -> [l4="v4":C]
  //     }
  //     (l1, l3, l2) -> {
  //         (v1, v3) -> [l2=~"v2.*":E]
  //     }
  //     (l1, l2) -> {
  //         (v1) -> [l2="v2a":F, l2="v2b":G]
  //         (v1a) -> [l2="v2a":H]
  //     }
  //     (l1) -> {
  //         () -> [l1="v1":I]
  //     }

  /**
   * Maps individual filters to elements.
   * Equals filters are stored as a Map for efficiency.
   */
  protected case class FilterToElementMap(equalsFilters: Map[String, T],
                                          otherFilters: Iterable[(ColumnFilter, T)]) {
    /**
     * Returns an occupied Option iff at least one filter is matched.
     * The Equals map is checked first before other filters are iterated.
     *
     * @param string the string to be matched.
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

  // Stores a map for each possible sequence of labels.
  // Keys are found deterministically from each set of ColumnFilters to store.
  val labelSeqToMap = new mutable.HashMap[Seq[String], mutable.Map[Seq[String], FilterToElementMap]]

  filterElementPairs.foreach{ case (filters, _) =>
    require(filters.count(!_.filter.isInstanceOf[Filter.Equals]) <= 1,
      "at most one filter can be non-Equals; filters=" + filters)
  }

  filterElementPairs.map{ case (filters, elt) =>
    // Sort the filters to determine the appropriate labelSeqToMap key.
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
    //   - its sequence of label names (determines the map it will be assigned to)
    //   - its sequence of label values (determines the key for storage within the assigned map)
    val labelNames = sortedFilters.map(_.column)
    val prefixValues = sortedFilters
      .dropRight(1)
      .map(_.filter.asInstanceOf[Filter.Equals].value.toString)
    (labelNames, prefixValues)
  }.foreach{ case ((labelNames, prefixValues), sortedFiltersToElements) =>
    // All (filters,element) pairs here will be grouped at the same path in the same map.
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
    labelSeqToMap
      .getOrElseUpdate(labelNames, new mutable.HashMap[Seq[String], FilterToElementMap]())
      .put(prefixValues, filterToElementMap)
  }

  /**
   * Returns an occupied Option iff the labels matched an
   *   entire set of ColumnFilters.
   * An element is chosen arbitrarily when multiple sets are matched.
   *
   * Runtime grows (among other ways):
   *     - linearly with the max count of filters in a key set.
   *     - linearly with the count of distinct sets of key filter label names.
   *   This data-structure should only be used where each of these is reasonably small.
   */
  def get(labels: collection.Map[String, String]): Option[T] = {
    // The maps we've built require that certain labels are present in the set.
    // Identify the set of (map, labels-names) pairs s.t. all labels are present,
    //   then map these to the set of (map, label-values) pairs for efficiency.
    val mapValuesPairs = labelSeqToMap
      .map{ case (filterLabels, map) => (map, filterLabels.map(labels.get)) }  // Get all value options.
      .filter { case (_, filterValues) => filterValues.forall(_.isDefined) }   // Make sure all are defined.
      .map { case (map, filterValues) => (map, filterValues.map(_.get)) }      // Extract the values.
    for ((map, values) <- mapValuesPairs) {
      // Get the set of final ColumnFilters from the map, then
      //   check each against the final label value.
      val valuesPrefix = values.dropRight(1)
      val matchedElement = map.get(valuesPrefix)
        .flatMap(filterToElementMap => filterToElementMap.get(values.last))
      if (matchedElement.isDefined) {
        return matchedElement
      }
    }
    None  // No matches were found.
  }

  override def toString(): String = {
    s"ColumnFilterMap(${labelSeqToMap.toString()})"
  }
}