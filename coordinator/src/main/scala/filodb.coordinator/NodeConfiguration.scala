package filodb.coordinator

import com.typesafe.config.Config

import filodb.core.GlobalConfig


/** Mixin used for nodes and tests. */
trait NodeConfiguration {

  /** The global Filo configuration. */
  val systemConfig: Config = GlobalConfig.systemConfig

}


