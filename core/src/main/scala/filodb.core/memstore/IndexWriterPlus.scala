package filodb.core.memstore

import kamon.Kamon
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig, MergePolicy}
import org.apache.lucene.store.Directory

import filodb.core.DatasetRef

class IndexWriterPlus(d: Directory,
                      conf: IndexWriterConfig,
                      ref: DatasetRef,
                      shardNum: Int) extends IndexWriter(d, conf) {

  val mergingNumBytesInProgress = Kamon.gauge("index-num-bytes-merging-in-progress")
    .withTag("dataset", ref.dataset)
    .withTag("shard", shardNum)

  val mergingNumDocsInProgress = Kamon.gauge("index-num-docs-merging-in-progress")
    .withTag("dataset", ref.dataset)
    .withTag("shard", shardNum)

  override def merge(merge: MergePolicy.OneMerge): Unit = {
    val numDocs = merge.totalNumDocs()
    val numBytes = merge.totalBytesSize()
    mergingNumDocsInProgress.increment(numDocs)
    mergingNumBytesInProgress.increment(numBytes)
    try {
      super.merge(merge)
    } finally {
      mergingNumDocsInProgress.decrement(numDocs)
      mergingNumBytesInProgress.decrement(numBytes)
    }
  }
}
