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
 * NOTE: This implementation is intended to favor simplicity over efficiency.
 *   Other implementations should be considered if the {@link #get} runtime
 *   (described in the method's javadoc) is insufficient.
 *
 * @tparam T the type of elements to store/return.
 * @param filterElementPairs (filter-set,element) pairs
 */
class ColumnFilterMap[T](filterElementPairs: Iterable[(Iterable[ColumnFilter], T)]){

  // HashMaps are used to efficiently match all Equals filters. To do this, we use two "layers" of maps.
  //     - "Outer" maps map sequences of Equals filter names to "inner" maps.
  //     - "Inner" maps map sequences of Equals filter values to Either[<regex-filters>, <element>]
  //   Outer map keys are found by filtering for Equals filters then sorting by column name alphabetically:
  //     1) {labelA="value1a", labelB="value2a"} -> (labelA, labelB)
  //     2) {labelA="value1b", labelB="value2b"} -> (labelA, labelB)  // Same as above.
  //     3) {labelC=~"value3", labelD="value4", labelE="value5"} -> (labelD, labelE)  // Regex columns are dropped.
  //     4) {labelC=~"value3", labelD=~"value4", labelE=~"value5"} -> ()  // All are regex.
  //   Inner map keys are simply the corresponding values to the outer-key labels:
  //     1) {labelA="value1a", labelB="value2a"} -> (value1a, value2a)
  //     2) {labelA="value1b", labelB="value2b"} -> (value1b, value2b)
  //     3) {labelC=~"value3", labelD="value4", labelE="value5"} -> (value4, value5)
  //     4) {labelC=~"value3", labelD=~"value4", labelE=~"value5"} -> ()
  // When a set of labels-value pairs is processed through the ColumnFilterMap, each inner map is iterated:
  //   the appropriate N-tuple of values is extracted from the argument label-value pairs and used to access
  //   each map. When an inner key exists, we inspect the Either: if an element is contained, that is
  //   returned directly. Otherwise, sets of filters are iterated until a set is found s.t. all are matched
  //   by the argument label-value pairs; this element is returned. If no set is matched completely,
  //   None is returned.
  //
  // As an example, consider a ColumnFilterMap composed of these entries:
  //
  //     {l1="v1", l2="v2", l3="v3a"} -> A
  //     {l1="v1", l2="v2", l3="v3b"} -> B
  //     {l1="v1", l2="v2", l4="v4"} -> C
  //     {l1="v1", l2=~"v2.*", l3="v3"} -> D
  //     {l1=~"v1a.*", l2="v2"} -> E
  //     {l1=~"v1b.*", l2="v2"} -> F
  //     {l1=~"v1.*", l2=~"v2.*", l3=~"v3.*"} -> G
  //
  //   The nested maps would be structured as:
  //
  //     (l1, l2, l3) -> {
  //         (v1, v2, v3a) -> Either[A]
  //         (v1, v2, v3b) -> Either[B]
  //     }
  //     (l1, l2, l4) -> {
  //         (v1, v2, v4) -> Either[C]
  //     }
  //     (l1, l3) -> {
  //         (v1, v3) -> Either[([l2=~"v2.*"], D)]
  //     }
  //     (l2) -> {
  //         (v2) -> Either[([l1=~"v1a.*"], E),([l1=~"v1b.*"], F)]
  //     }
  //     () -> {
  //         () -> Either[([l1=~"v1.*", l2=~"v2.*", l3=~"v3.*"], G)]
  //     }

  // Stores a map for each sequence of Equals labels.
  // (l1, l2, l3) -> {
  //     (v1, v2, v3) -> Either[Iterable[(filters, elt)], elt]
  // }
  val labelSeqToMap =
    new mutable.HashMap[Seq[String], mutable.Map[Seq[String], Either[Iterable[(Iterable[ColumnFilter], T)], T]]]

  filterElementPairs.groupBy{ case (filters, elt) =>
    // Group each set of filters by:
    //   - its sorted sequence of Equals label names (determines the map it will be assigned to)
    //   - its corresponding sequence of label values (determines the key for storage within the assigned map)
    val sortedEqualsFilters = filters
      .filter(colFilter => colFilter.filter.isInstanceOf[Filter.Equals])
      .toSeq
      .sortBy(_.column)
    val equalsLabels = sortedEqualsFilters.map(_.column)
    val equalsValues = sortedEqualsFilters.map(_.filter.asInstanceOf[Filter.Equals].value.toString)
    (equalsLabels, equalsValues)
  }.foreach{ case ((equalsLabels, equalsValues), filtersToElements) =>
    // All (filters,element) pairs here will be grouped at the same key in the same map.
    // Note that we don't need to store any ColumnFilters here if any single set contains all Equals filters;
    //   all these filters will have been matched by the map keys, so we can just return the corresponding
    //   element by default. Therefore, we'll use Either below to store either a plain element
    //   or a set of (filter-set,element).
    val nonEqualsFiltersToElt = new ArrayBuffer[(Iterable[ColumnFilter], T)]
    val allEqualsFiltersOpt = filtersToElements
      .map{ case (filters, elt) =>
        val nonEqualsFilters = filters.filterNot(_.filter.isInstanceOf[Filter.Equals])
        nonEqualsFiltersToElt.append((nonEqualsFilters, elt))
        (filters, elt)
      }
      .find{ case (filters, _) =>
        filters.forall(_.filter.isInstanceOf[Filter.Equals])
      }

    val either: Either[Iterable[(Iterable[ColumnFilter], T)], T] = allEqualsFiltersOpt match {
      case Some((filters, elt)) => Right(elt)
      case None => Left(nonEqualsFiltersToElt)
    }

    labelSeqToMap
      .getOrElseUpdate(
        equalsLabels,
        new mutable.HashMap[Seq[String], Either[Iterable[(Iterable[ColumnFilter], T)], T]]())
      .put(equalsValues, either)
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
    labelSeqToMap
      .map{ case (filterLabels, map) => (map, filterLabels.map(labels.get)) }  // Get all value options.
      .filter { case (_, filterValues) => filterValues.forall(_.isDefined) }   // Make sure all are defined.
      .map { case (map, filterValues) => (map, filterValues.map(_.get)) }      // Extract the values.
      .map { case (valuesMap, values) =>
        valuesMap.get(values).flatMap {
          case Left(filtersToElts) =>
            filtersToElts.find { case (filters, elt) =>
              // Make the argument label-value pairs:
              //   - contains labels for all filter columns.
              //   - matches all filters.
              filters.forall { filter =>
                labels.get(filter.column).exists(value => filter.filter.filterFunc(value))
              }
            }.map { case (filters, elt) => elt }
          case Right(elt) =>
            // No need to evaluate any filters; some set of filters used to build the map
            //   was entirely composed of Equals filters, and the fact that the valuesMap
            //   returned a value implies that all filters were matched.
            Some(elt)
        }
      }
      .filter(_.isDefined)
      .map(_.get)
      .headOption  // Although multiple matches are possible, we just return the first.
  }

  override def toString(): String = {
    s"ColumnFilterMap(${labelSeqToMap.toString()})"
  }
}