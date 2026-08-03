package filodb.downsampler

import com.datastax.driver.core.Session
import com.typesafe.config.Config
import com.typesafe.scalalogging.{Logger, StrictLogging}
import monix.execution.Scheduler

import filodb.cassandra.FiloSessionProvider
import filodb.cassandra.columnstore.CassandraColumnStore
import filodb.core.concurrentCache

object DownsamplerContext extends StrictLogging {
  lazy protected[downsampler] val dsLogger: Logger = logger
  lazy protected[downsampler] val sessionMap = concurrentCache[Config, Session](2)

  lazy protected[downsampler] val readSched = Scheduler.io("cass-read-sched")
  lazy protected[downsampler] val writeSched = Scheduler.io("cass-write-sched")

  lazy protected[downsampler] val colStoreMap = concurrentCache[Config, CassandraColumnStore](2)

  def getOrCreateCassandraColumnStore(filodbConfig: Config, cassandraConfig: Config)
                                     (implicit sched: Scheduler): CassandraColumnStore = {
    colStoreMap.getOrElseUpdate(filodbConfig, { conf =>
      dsLogger.info(s"Creating new CassandraColumnStore")
      val session = getOrCreateCassandraSession(cassandraConfig)
      new CassandraColumnStore(conf, sched, session, false)(sched)
    })
  }

  def getOrCreateCassandraSession(config: Config): Session = {
    import filodb.core._
    sessionMap.getOrElseUpdate(config, { conf =>
      dsLogger.info(s"Creating new Cassandra session")
      FiloSessionProvider.openSession(conf)
    })
  }
}
