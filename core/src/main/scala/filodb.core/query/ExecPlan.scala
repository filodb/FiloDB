package filodb.core.query

import monix.eval.Task
import monix.reactive.Observable

import filodb.core.binaryrecord.{BinaryRecord, RecordSchema}
import filodb.core.metadata.Dataset
import filodb.core.store._
import filodb.core.Types
import filodb.memory.format.FastFiloRowReader

/**
 * An ExecPlan describes an executable query plan that FiloDB processes itself can carry out.
 * More precisely, it is one node in a tree of nodes, each of which can execute its part of the query.
 * Every ExecPlan is part of a flow from children's outputs and yields its own output type.
 * Each ExecPlan node can also transform the schema.
 */
abstract class ExecPlan[I: ResultMaker, O: ResultMaker]{
  def children: Seq[ExecPlan[_, I]]

  /**
   * The full type string for I and O including inner erased classes.  They may be used to typecheck
   * the tree during validation.
   */
  final def inputType: String = implicitly[ResultMaker[I]].typeInfo
  final def outputType: String = implicitly[ResultMaker[O]].typeInfo

  /** Expected to produce the given output from the children's outputs (which are the inputs) */
  def execute(source: ChunkSource, dataset: Dataset): O

  /** Should return the schema of the output Vector or Tuple */
  def schema(dataset: Dataset): ResultSchema

  /** Should return the chunkMethod needed to limit return output */
  def chunkMethod: ChunkScanMethod

  /**
   * Executes the plan, including all children, and returns the Result
   * @param limit the result size taken from Observables to no more than limit items
   */
  def executeToResult(source: ChunkSource,
                      dataset: Dataset,
                      limit: Int = 1000): Task[Result] =
    implicitly[ResultMaker[O]].toResult(execute(source, dataset), schema(dataset), chunkMethod, limit)

  /** Arguments for this ExecPlan for printTree/debugging output */
  def args: Seq[String]

  def printTree(level: Int = 0): String =
    s"${"  "*level}${getClass.getSimpleName} => $outputType  (${args.mkString(", ")})\n" +
    s"${children.map(_.printTree(level + 1)).mkString("\n")}"

  override def toString: String = printTree(0)
}

/**
 * NOTE: These concepts mirror the ones in Spark's SparkPlan
 * A LeafExecNode has no inputs, it only gives output.
 */
abstract class LeafExecNode[O: ResultMaker] extends ExecPlan[Unit, O] {
  override final def children: Seq[ExecPlan[_, Unit]] = Nil
}

abstract class UnaryExecNode[I: ResultMaker, O: ResultMaker] extends ExecPlan[I, O] {
  def child: ExecPlan[_, I]

  override final def children: Seq[ExecPlan[_, I]] = child :: Nil
}

abstract class MultiExecNode[I: ResultMaker, O: ResultMaker](val children: Seq[ExecPlan[_, I]]) extends ExecPlan[I, O] {
  require(children.length > 0, "Must have at least one child")

  val chunkMethod = children.head.chunkMethod
  require(children.tail.forall(_.chunkMethod == chunkMethod))

  def schema(dataset: Dataset): ResultSchema = {
    val firstSchema = children.head.schema(dataset)
    require(children.tail.forall(_.schema(dataset) == firstSchema), s"Children schema disagree!")
    firstSchema
  }
}

/**
 * A complete, generic ExecPlan with one child and a function mapping the child output I to this one's output O.
 */
class MappedUnaryExecNode[I: ResultMaker, O: ResultMaker](mapFn: I => O, val child: ExecPlan[_, I])
                                                         (schemaFn: ResultSchema => ResultSchema = x => x)
extends UnaryExecNode[I, O] {
  def execute(source: ChunkSource, dataset: Dataset): O = mapFn(child.execute(source, dataset))
  def schema(dataset: Dataset): ResultSchema = schemaFn(child.schema(dataset))
  def chunkMethod: ChunkScanMethod = child.chunkMethod
  def args: Seq[String] = Seq(mapFn.toString, schemaFn.toString)
}

object ExecPlan {
  // Maps Observable[I] to Observable[O] using I => O
  class ObservableMapper[I, O](mapFn: I => O, child: ExecPlan[_, Observable[I]])
                              (implicit iMaker: ResultMaker[Observable[I]], oMaker: ResultMaker[Observable[O]])
    extends MappedUnaryExecNode[Observable[I], Observable[O]](_.map(mapFn), child)() {
    override def args: Seq[String] = Seq(mapFn.toString)
  }

  type PartitionsMapper  = ObservableMapper[PartitionVector, PartitionVector]
  type PartitionsReducer = ObservableMapper[PartitionVector, Tuple]
  type TuplesMapper      = ObservableMapper[Tuple, Tuple]

  // Function on just one tuple, for example * constant
  type OneTupleFunction  = MappedUnaryExecNode[Tuple, Tuple]

  // Folds Observable[I] => aggregate A
  class ObservableFolder[I, A](foldFn: (A, I) => A,
                               initValue: A,
                               child: ExecPlan[_, Observable[I]])
                              (implicit iMaker: ResultMaker[Observable[I]], aMaker: ResultMaker[Task[A]])
    extends MappedUnaryExecNode[Observable[I], Task[A]](_.foldLeftL(initValue)(foldFn), child)()


  /**
   * The mother of all leaf nodes, returns a stream of PartitionVectors
   * SORRY- we cannot use logger in here because it will get serialized which is a no no
   *
   * @param columnIDs the exact columns to return as vectors.  If this is a ranged vector query, then the first IDs
   *                  MUST be the dataset row key IDs, otherwise the range selection in the result won't work
   */
  class LocalVectorReader(columnIDs: Seq[Types.ColumnId],
                          partMethod: PartitionScanMethod,
                          val chunkMethod: ChunkScanMethod) extends LeafExecNode[Observable[PartitionVector]] {
    def execute(source: ChunkSource, dataset: Dataset): Observable[PartitionVector] = {
      chunkMethod match {
        case r: RowKeyChunkScan => require(columnIDs.indexOfSlice(dataset.rowKeyIDs) == 0)
        case _ =>
      }
      source.partitionVectors(dataset, columnIDs, partMethod, chunkMethod)
    }

    def schema(dataset: Dataset): ResultSchema =
      ResultSchema(dataset.infosFromIDs(columnIDs),
                   columnIDs.zip(dataset.rowKeyIDs).takeWhile { case (a, b) => a == b }.length)

    def args: Seq[String] = Seq(columnIDs.toString, partMethod.toString, chunkMethod.toString)
  }

  def lastTupleFn(sourceSchema: Seq[ColumnInfo]): PartitionVector => Tuple = {
    val recSchema = new RecordSchema(sourceSchema.map(_.colType))

    { partVect =>
      if (partVect.readers.isEmpty || partVect.readers.head.info.numRows == 0) {
        Tuple(partVect.info, BinaryRecord.empty)
      } else {
        val chunkSet  = partVect.readers.last
        val rowReader = new FastFiloRowReader(chunkSet.vectors)
        rowReader.setRowNo(chunkSet.info.numRows - 1)    // last row
        Tuple(partVect.info, BinaryRecord(recSchema, rowReader))
      }
    }
  }

  /**
   * Creates an ExecPlan that streams back the last tuple of every partition, basically
   * {{{
   *   ReducePartitions(lastTupleFn,
   *     LocalVectorReader(...))
   * }}}
   */
  def streamLastTuplePlan(dataset: Dataset,
                          columnIDs: Seq[Types.ColumnId],
                          partMethod: PartitionScanMethod): PartitionsReducer =
    new PartitionsReducer(lastTupleFn(dataset.infosFromIDs(columnIDs)),
      new LocalVectorReader(columnIDs, partMethod, LastSampleChunkScan))
}

