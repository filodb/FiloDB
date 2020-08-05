package filodb.core.memstore.ratelimit

import filodb.core.DatasetRef

/**
 * Allows for action to be taken when quota is breached
 */
trait QuotaExceededProtocol {
  /**
   * Invoked when quota is breached.
   *
   * Example actions that could be taken:
   * 1. Send message to gateway to block ingestion of this shardKeyPrefix
   * 2. Automatically increase quota if reasonable
   * 3. Send a notification/alert to customers or operations
   *
   * Note that this can be invoked multiple times until either ingestion of invalid data stops
   * or if quota is fixed. So implementations should ensure that actions are idempotent.
   *
   * @param ref dataset
   * @param shardNum the shardNumber that breached the quota
   * @param shardKeyPrefix the shardKeyPrefix for which quota was breached
   * @param quota the actual quota number that was breached
   */
  def quotaExceeded(ref: DatasetRef, shardNum: Int, shardKeyPrefix: Seq[String], quota: Int): Unit
}

/**
 * Default implementation which takes no action.
 */
object NoActionQuotaProtocol extends QuotaExceededProtocol {
  def quotaExceeded(ref: DatasetRef, shardNum: Int, shardKeyPrefix: Seq[String], quota: Int): Unit = {}
}