package filodb.query.exec

import java.util.concurrent.atomic.AtomicInteger

import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

import filodb.core.DatasetRef
import filodb.core.metadata.Column.ColumnType
import filodb.core.query._
import filodb.core.query.NoCloseCursor.NoCloseCursor
import filodb.core.store.ChunkSource
import filodb.memory.EvictionLock
import filodb.memory.format.ZeroCopyUTF8String._
import filodb.query.{QueryError, QueryResponse, QueryResult}

/**
 * Results of in-process children dispatched with preventRangeVectorSerialization are lazy RVs that read
 * block memory when the parent iterates them. The eviction lock acquired by the child must therefore stay
 * held until the parent's session closes, not just until the child's task completes.
 */
class InProcessDispatchEvictionLockSpec extends AnyFunSpec with Matchers with ScalaFutures {
  implicit val defaultPatience: PatienceConfig = PatienceConfig(timeout = Span(10, Seconds),
    interval = Span(50, Millis))

  private val queryConfig = QueryConfig.unitTestingQueryConfig.copy(enableLocalDispatch = true)
  private val dispatcher = InProcessPlanDispatcher(queryConfig)
  private val source = UnsupportedChunkSource()
  private val schema = ResultSchema(Seq(ColumnInfo("timestamp", ColumnType.TimestampColumn),
                                        ColumnInfo("value", ColumnType.DoubleColumn)), 1)

  /** True if some query holds the lock in shared mode; probes with a 1ms exclusive attempt. */
  private def isHeldShared(lock: EvictionLock): Boolean = {
    val acquired = lock.tryExclusiveReclaimLock(1)
    if (acquired) lock.releaseExclusive()
    !acquired
  }

  /**
   * Leaf that acquires `lock` like MultiSchemaPartitionsExec does, and returns a lazy RV whose rows()
   * records whether the lock was still held when the RV was actually iterated.
   */
  private class LockingLeafPlan(val queryContext: QueryContext,
                                lock: EvictionLock,
                                iteratedWithoutLock: AtomicInteger) extends LeafExecPlan {
    override protected def args: String = "locking-leaf"
    override def dataset: DatasetRef = DatasetRef("test")
    override def dispatcher: PlanDispatcher = InProcessDispatchEvictionLockSpec.this.dispatcher
    override def doExecute(source: ChunkSource, querySession: QuerySession)
                          (implicit sched: Scheduler): ExecResult = {
      lock.acquireSharedLock(1000, queryContext.queryId, "test") shouldEqual true
      querySession.setLock(lock)
      val rv = new RangeVector {
        override def key: RangeVectorKey = CustomRangeVectorKey(Map("foo".utf8 -> "bar".utf8))
        override def rows(): RangeVectorCursor = {
          if (!isHeldShared(lock)) iteratedWithoutLock.incrementAndGet()
          new NoCloseCursor((0 until 10).map(i => new TransientRow(i.toLong, i.toDouble)).iterator)
        }
        override def outputRange: Option[RvRange] = None
      }
      ExecResult(Observable.now(rv), Task.now(schema))
    }
  }

  private def newSession(prevent: Boolean = false) =
    QuerySession(QueryContext(), queryConfig, catchMultipleLockSetErrors = true,
      preventRangeVectorSerialization = prevent)

  private def dispatchLeaf(leaf: ExecPlan, parent: QuerySession, prevent: Boolean): QueryResponse =
    dispatcher.dispatch(ExecPlanWithClientParams(leaf, ClientParams(10000, prevent), parent), source)
      .runToFuture.futureValue

  it("should hand the child's eviction lock to the parent session when RVs are not serialized") {
    val lock = new EvictionLock()
    val parent = newSession()
    val notLocked = new AtomicInteger(0)
    dispatchLeaf(new LockingLeafPlan(QueryContext(), lock, notLocked), parent, prevent = true) match {
      case r: QueryResult =>
        r.result.head.isInstanceOf[SerializedRangeVector] shouldEqual false
        isHeldShared(lock) shouldEqual true // child is done, but its lazy RV has not been read yet
        r.result.head.rows().size shouldEqual 10
      case e: QueryError => throw e.t
    }
    notLocked.get shouldEqual 0
    parent.adoptedLocks.numHeld shouldEqual 1
    parent.close()
    isHeldShared(lock) shouldEqual false
  }

  it("should release the child's eviction lock when the child completes if RVs are serialized") {
    val lock = new EvictionLock()
    val parent = newSession()
    dispatchLeaf(new LockingLeafPlan(QueryContext(), lock, new AtomicInteger(0)), parent, prevent = false) match {
      case r: QueryResult => r.result.head.isInstanceOf[SerializedRangeVector] shouldEqual true
      case e: QueryError  => throw e.t
    }
    isHeldShared(lock) shouldEqual false
    parent.adoptedLocks.numHeld shouldEqual 0
    parent.close()
  }

  it("should keep the leaf locks held while the parent composes local-dispatch child results") {
    val lock = new EvictionLock()
    val notLocked = new AtomicInteger(0)
    val leaves = Seq(new LockingLeafPlan(QueryContext(), lock, notLocked),
                     new LockingLeafPlan(QueryContext(), lock, notLocked))
    val stitch = StitchRvsExec(QueryContext(), dispatcher, None, leaves)
    val top = newSession()
    dispatcher.dispatch(ExecPlanWithClientParams(stitch, ClientParams(10000), top), source)
      .runToFuture.futureValue match {
      case r: QueryResult => r.result.head.rows().size shouldEqual 10
      case e: QueryError  => throw e.t
    }
    // leaf RVs were iterated by the parent (stitch + serialization) while the leaf locks were still held
    notLocked.get shouldEqual 0
    // the parent's own session closed when its dispatch completed, releasing the adopted leaf locks
    isHeldShared(lock) shouldEqual false
    top.close()
  }

  it("should share adopted locks between a session and its copies") {
    val lock = new EvictionLock()
    val parent = newSession()
    val child = newSession(prevent = true)
    lock.acquireSharedLock(1000, "q", "test") shouldEqual true
    child.setLock(lock)
    // children are dispatched with a copy of the parent's session
    child.closeAndTransferLocks(parent.copy(preventRangeVectorSerialization = true))
    isHeldShared(lock) shouldEqual true
    parent.close()
    isHeldShared(lock) shouldEqual false
  }

  it("should release immediately locks handed to a parent session that has already closed") {
    val lock = new EvictionLock()
    val parent = newSession()
    parent.close() // e.g. parent cancelled on timeout while the child was still running
    val child = newSession(prevent = true)
    lock.acquireSharedLock(1000, "q", "test") shouldEqual true
    child.setLock(lock)
    child.closeAndTransferLocks(parent)
    isHeldShared(lock) shouldEqual false
  }

  it("should forward locks adopted from grandchildren when transferring to the parent") {
    val lock1 = new EvictionLock()
    val lock2 = new EvictionLock()
    val parent = newSession()
    val child = newSession(prevent = true)
    val grandChild = newSession(prevent = true)
    lock1.acquireSharedLock(1000, "q", "test") shouldEqual true
    lock2.acquireSharedLock(1000, "q", "test") shouldEqual true
    grandChild.setLock(lock1)
    child.setLock(lock2)
    grandChild.closeAndTransferLocks(child)
    child.closeAndTransferLocks(parent)
    isHeldShared(lock1) shouldEqual true
    isHeldShared(lock2) shouldEqual true
    parent.adoptedLocks.numHeld shouldEqual 2
    parent.close()
    isHeldShared(lock1) shouldEqual false
    isHeldShared(lock2) shouldEqual false
    parent.close() // idempotent
  }
}
