package filodb.cassandra.metastore

import com.typesafe.config.Config
import filodb.core.Messages.Success
import filodb.core.metadata.Column
import filodb.core.store.{Dataset, MetaStore, ProjectionInfo}

import scala.concurrent.{ExecutionContext, Future}

/**
 * A class for Cassandra implementation of the MetaStore.
 *
 * @param config a Typesafe Config with hosts, port, and keyspace parameters for Cassandra connection
 */
class CassandraMetaStore(config: Config)
                        (implicit val ec: ExecutionContext) extends MetaStore {

  val projectionTable = new ProjectionTable(config)

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
