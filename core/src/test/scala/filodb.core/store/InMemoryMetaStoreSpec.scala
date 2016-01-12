package filodb.core.store

import org.scalatest.time.{Millis, Span, Seconds}

class InMemoryMetaStoreSpec extends MetaStoreSpec {
  implicit val defaultPatience =
    PatienceConfig(timeout = Span(10, Seconds), interval = Span(50, Millis))

  import scala.concurrent.ExecutionContext.Implicits.global
  lazy val metaStore = new InMemoryMetaStore
}