package filodb.coordinator.client

import com.typesafe.scalalogging.slf4j.StrictLogging
import scala.concurrent.duration._

import filodb.core._
import filodb.coordinator._

case object NoClusterActor extends ErrorResponse

trait ClusterOps extends ClientBase with StrictLogging {
  import NodeClusterActor._

  /**
   * Sets up the cluster for ingestion and querying of a dataset which must have been defined in the
   * MetaStore already.  Initiates pull ingestion using RowSources on each node that has been assigned
   * shards.
   * @param dataset the DatasetRef of the dataset to start ingesting
   * @param schema the column names in the exact order they will be presented in RowReaders sent to nodes
   * @param spec the DatasetResourceSpec specifying # of shards and nodes for ingest and querying
   * @param source the IngestionSource specifying where to pull data from
   * @return None upon success, Some(ErrorResponse) on error or if dataset already setup
   */
  def setupDataset(dataset: DatasetRef,
                   schema: Seq[String],
                   spec: DatasetResourceSpec,
                   source: IngestionSource,
                   timeout: FiniteDuration = 30.seconds): Option[ErrorResponse] =
    clusterActor.map { ref =>
      Client.actorAsk(ref, SetupDataset(dataset, schema, spec, source), timeout) {
        case DatasetVerified  => None
        case e: ErrorResponse => Some(e)
      }
    }.getOrElse(Some(NoClusterActor))
}