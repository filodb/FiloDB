package filodb.cassandra.columnstore

import java.io.DataInputStream
import java.nio.ByteBuffer

import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.StrictLogging
import filodb.core.Messages._
import filodb.core.Types.{ChunkId, ColumnId}
import filodb.core.metadata._
import filodb.core.store.{ChunkStore, ColumnStore}
import it.unimi.dsi.io.ByteBufferInputStream
import org.velvia.filo.FastFiloRowReader

import scala.concurrent.{ExecutionContext, Future}

/**
 * Implementation of a column store using Apache Cassandra tables.
 * This class must be thread-safe as it is intended to be used concurrently.
 *
 * Both the instances of the Segment* table classes above as well as ChunkRowMap entries
 * are cached for faster I/O.
 *
 * ==Configuration==
 * {{{
 *   cassandra {
 *     keyspace = "my_cass_keyspace"
 *   }
 *   columnstore {
 *     tablecache-size = 50    # Number of cache entries for C* for ChunkTable etc.
 *     segment-cache-size = 1000    # Number of segments to cache
 *   }
 * }}}
 *
 * ==Constructor Args==
 * @param config see the Configuration section above for the needed config
 * @param ec An ExecutionContext for futures.  See this for a way to do backpressure with futures:
 *           http://quantifind.com/blog/2015/06/throttling-instantiations-of-scala-futures-1/
 */
class CassandraColumnStore(config: Config)
                          (implicit val ec: ExecutionContext)
  extends ColumnStore with StrictLogging {

}

