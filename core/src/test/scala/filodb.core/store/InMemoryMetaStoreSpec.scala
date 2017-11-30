package filodb.core.store

import org.scalatest.time.{Millis, Seconds, Span}

class InMemoryMetaStoreSpec extends MetaStoreSpec {
  implicit override val defaultPatience =
    PatienceConfig(timeout = Span(20, Seconds), interval = Span(250, Millis))

  import scala.concurrent.ExecutionContext.Implicits.global
  lazy val metaStore = new InMemoryMetaStore
}