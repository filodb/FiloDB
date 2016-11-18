package filodb.core.store

class InMemoryColumnStoreSpec extends ColumnStoreSpec {
  import scala.concurrent.ExecutionContext.Implicits.global
  val colStore = new InMemoryColumnStore(global)
}