package com.mchange.feedletter

import javax.sql.DataSource

object Config:
  final case class Feed( feedUrl : String, minDelaySeconds : Long, awaitStabilizationSeconds : Long )
case class Config( dbName : String, dumpDir : os.Path, feeds : Set[Config.Feed] )








