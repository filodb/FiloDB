package filodb.core.memstore

import org.apache.lucene.index.{IndexWriter, IndexWriterConfig, MergePolicy}
import org.apache.lucene.store.Directory

import filodb.core.DatasetRef
import filodb.core.metrics.FilodbMetrics

class IndexWriterPlus(d: Directory,
                      conf: IndexWriterConfig,
                      ref: DatasetRef,
                      shardNum: Int) extends IndexWriter(d, conf) {

  val mergingNumBytesInProgress = FilodbMetrics.upDownCounter("index-num-bytes-merging-in-progress",
    Map("dataset" -> ref.dataset, "shard" -> shardNum.toString))

  val mergingNumDocsInProgress = FilodbMetrics.upDownCounter("index-num-docs-merging-in-progress",
    Map("dataset" -> ref.dataset, "shard" -> shardNum.toString))

  override def merge(merge: MergePolicy.OneMerge): Unit = {
    val numDocs = merge.totalNumDocs()
    val numBytes = merge.totalBytesSize()
    mergingNumDocsInProgress.increment(numDocs)
    mergingNumBytesInProgress.increment(numBytes)
    try {
      super.merge(merge)
    } finally {
      mergingNumDocsInProgress.increment(-numDocs)
      mergingNumBytesInProgress.increment(-numBytes)
    }
  }
}
