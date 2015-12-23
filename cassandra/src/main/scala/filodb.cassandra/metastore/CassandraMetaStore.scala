package filodb.cassandra.metastore

import com.datastax.driver.core.Session
import com.websudos.phantom.connectors.KeySpace
import filodb.core.Messages.{Response, Success}
import filodb.core.metadata.Column
import filodb.core.store.{Dataset, MetaStore, ProjectionInfo}
import filodb.core.util.FiloLogging

import scala.concurrent.{ExecutionContext, Future}

/**
 * A class for Cassandra implementation of the MetaStore.
 *
 */
class CassandraMetaStore(keySpace: KeySpace, session: Session)
                        (implicit val ec: ExecutionContext) extends MetaStore
with FiloLogging {

  val projectionTable = new ProjectionTable(keySpace, session)

  def initialize: Future[Seq[Response]] = {
    flow.warn(s"Initializing meta store")
    Future sequence Seq(projectionTable.initialize())
  }

  def clearAll: Future[Seq[Response]] = {
    flow.warn(s"Removing all tables")
    Future sequence Seq(projectionTable.clearAll())
  }

  /**
   * Retrieves a Dataset object of the given name
   * @param name Name of the dataset to retrieve
   * @return a Dataset
   */
  override def getDataset(name: String): Future[Option[Dataset]] = {
    for {
      projections <- projectionTable.getProjections(name)
      dataset = if (projections.isEmpty) {
        None
      } else {
        Some(Dataset(name, projections.head.schema, projections))
      }
    } yield dataset
  }

  override def addProjection(projectionInfo: ProjectionInfo): Future[Boolean] = {
    flow.debug(s"Creating projection $projectionInfo")
    for {
      inserted <- projectionTable.insertProjection(projectionInfo)
      result = inserted match {
        case Success => true
        case _ => false
      }
    } yield result
  }

  /**
   * Get the schema for a version of a dataset.  This scans all defined columns from the first version
   * on up to figure out the changes. Deleted columns are not returned.
   * Implementations should use Column.foldSchema.
   * @param dataset the name of the dataset to return the schema for
   * @return a Schema, column name -> Column definition, or ErrorResponse
   */
  override def getSchema(dataset: String): Future[Seq[Column]] = {
    projectionTable.getSuperProjection(dataset).map(_.schema)
  }

  override def getProjection(name: String, projectionId: Int): Future[ProjectionInfo] = {
    projectionTable.getProjection(name, projectionId)
  }
}
