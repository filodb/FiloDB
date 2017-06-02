package filodb.core.store

import org.scalatest.DoNotDiscover

@DoNotDiscover
class InMemoryColumnStoreSpec extends ColumnStoreSpec {
  import scala.concurrent.ExecutionContext.Implicits.global
  val colStore = new InMemoryColumnStore(global)
}