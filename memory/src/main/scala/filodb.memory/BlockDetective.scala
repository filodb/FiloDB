package filodb.memory

import org.joda.time.DateTime

/**
 * BlockDetective has utilities for recovering blocks and their reclaim histories, given a corrupt pointer.
 * This helps to track down ownership and causes for corruption issues.
 */
object BlockDetective {
  import collection.JavaConverters._

  def containsPtr(ptr: BinaryRegion.NativePointer, blocks: Seq[Block]): Seq[Block] =
    blocks.filter { blk => ptr >= blk.address && ptr < (blk.address + blk.capacity) }

  def containsPtr(ptr: BinaryRegion.NativePointer, blocks: java.util.List[Block]): Seq[Block] =
    containsPtr(ptr, blocks.asScala)

  /**
   * Produces a string report containing reclaim history and ownership changes for
   * blocks containing a given pointer.
   * Reclaim history is limited by MaxReclaimLogSize above, thus
   * the bet is that corruption happens soon after reclaim events.
   */
  def stringReport(ptr: BinaryRegion.NativePointer,
                   manager: PageAlignedBlockManager,
                   pool: BlockMemFactoryPool): String = {
    val reclaimEvents = manager.reclaimEventsForPtr(ptr)
    val timeBucketBlocks = manager.timeBlocksForPtr(ptr)
    val flushBlocks = pool.blocksContainingPtr(ptr)

    f"=== BlockDetective Report for 0x$ptr%016x ===\nReclaim Events:\n" +
    reclaimEvents.map { case ReclaimEvent(blk, reclaimTime, oldOwner, remaining) =>
      f"  Block 0x${blk.address}%016x at ${(new DateTime(reclaimTime)).toString()}%s with $remaining%d bytes left" +
      oldOwner.map { bmf => s"\tfrom ${bmf.debugString}" }.getOrElse("")
    }.mkString("\n") +
    "\nTime bucketed blocks:\n" +
    timeBucketBlocks.map(_.debugString).mkString("\n") +
    "\nFlush block lists:\n" +
    flushBlocks.map(_.debugString).mkString("\n")
  }
}
