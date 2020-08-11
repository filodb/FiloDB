/**
  * Copyright (C) 2016- Yuske Yasuda
  * Copyright (C) 2019- SISIOH Project
  */
package org.sisioh.akka.cluster.custom.downing

/**
  * Thrown when split brain is resolved and resolved member should shutdown itself.
  * This error is fired in DowningProvider and handled by ClusterCoreSupervisor, which leads to cluster shutdown.
  * @param strategyName
  */
@SerialVersionUID(1L) class SplitBrainResolvedError(strategyName: String)
    extends Error(s"Resolution of split brain by $strategyName results in shutdown")
