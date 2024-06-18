package filodb.coordinator.queryplanner

import filodb.core.query.ActiveShardMapper

trait ActiveShardMapperProvider {
  def getActiveShardMapper(): ActiveShardMapper
}
