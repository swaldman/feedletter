package com.mchange.feedletter

import javax.sql.DataSource

object Config:
  final case class Feed( feedUrl : String, itemContentElement : String, minDelaySeconds : Long, maxDelaySeconds : Long, awaitStabilizationSeconds : Long )
case class Config( dataSource : DataSource, dbName : String, dumpDir : os.Path, feeds : Set[Config.Feed] )








