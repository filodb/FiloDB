package filodb.core.metadata

trait IngestionState {
  def nodeactor: String
  def database: String
  def dataset: String
  def version: Int
  def state: String
  def exceptions: String
}

/**
  * A class that holds real data.
  */
case class IngestionStateData(nodeactor: String,
                              database: String,
                              dataset: String,
                              version: Int,
                              state: String,
                              exceptions: String) extends IngestionState{

  override def toString: String =
    s"[$nodeactor,$database,$dataset,$version,${state},${exceptions}]"
}