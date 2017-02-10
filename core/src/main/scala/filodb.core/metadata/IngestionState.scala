package filodb.core.metadata

/**
  * A class that holds real data.
  */
case class IngestionStateData(nodeactor: String,
                              database: String,
                              dataset: String,
                              version: Int,
                              columns: String,
                              state: String,
                              exceptions: String) {
  override def toString: String =
    s"[${nodeactor}\u0001${database}\u0001${dataset}\u0001${version}" +
    s"\u0001${columns}\u0001${state}\u0001${exceptions}]"
}