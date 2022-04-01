package filodb.cassandra.columnstore

import scala.concurrent.{ExecutionContext, Future}
import com.datastax.driver.core.{ConsistencyLevel}
import filodb.cassandra.FiloCassandraConnector
import filodb.core.{DatasetRef, Response}
import monix.reactive.Observable

sealed class ShardKeyToPartKeyTable(val dataset: DatasetRef,
                                    val shard: Int,
                                    val connector: FiloCassandraConnector,
                                    writeConsistencyLevel: ConsistencyLevel)
                                   (implicit ec: ExecutionContext) extends BaseDatasetTable {

  import filodb.cassandra.Util._

  val suffix = s"shardKeyTopartKey_$shard"

  // TODO(a_theimer): compression settings okay?
  // TODO(a_theimer): probably need to prevent hot-spotting on a single node
  //   (i.e. add more fields to [Cassandra's] partition key)
  val createCql =
    s"""CREATE TABLE IF NOT EXISTS $tableString (
       |    shardKey blob,
       |    partKey blob,
       |    PRIMARY KEY (shardKey, partKey)
       |) WITH compression = {'chunk_length_in_kb': '16', 'sstable_compression': '$sstableCompression'}""".stripMargin

  private lazy val writePartitionCql = session.prepare(
      s"INSERT INTO ${tableString} (shardKey, partKey) " +
      s"VALUES (?, ?) USING TTL ?")
      .setConsistencyLevel(writeConsistencyLevel)

  private lazy val deleteCql = session.prepare(
    s"DELETE FROM $tableString " +
    s"WHERE shardKey = ? AND partKey = ?"
  )

  private lazy val scanCql = session.prepare(
    s"SELECT * FROM $tableString " +
      s"WHERE shardKey = ?")
    .setConsistencyLevel(ConsistencyLevel.ONE)

  def addMapping(shardKey: Array[Byte], partKey: Array[Byte], diskTtlSeconds: Long): Future[Response] = {
    connector.execStmt(writePartitionCql.bind(toBuffer(shardKey), toBuffer(partKey),
                       diskTtlSeconds.toInt: java.lang.Integer))
  }

  def deleteMappingNoAsync(shardKey: Array[Byte], partKey: Array[Byte]): Response = {
    val stmt = deleteCql.bind(toBuffer(shardKey), toBuffer(partKey))
                        .setConsistencyLevel(writeConsistencyLevel)
    connector.execCqlNoAsync(stmt)
  }

  def scanPartKeys(shardKey: Array[Byte]): Observable[Array[Byte]] = {
    val fut = session.executeAsync(scanCql.bind(toBuffer(shardKey))).toIterator.handleErrors
    Observable.fromFuture(fut)
     .flatMap(it => Observable.fromIterable(it.toSeq))
     .map(row => row.getBytes("partKey").array())
  }
}
