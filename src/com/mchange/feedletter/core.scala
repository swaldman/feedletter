package com.mchange.feedletter

object Config:
  final case class Feed( feedUrl : String, itemContentElement : String, minDelaySeconds : Long, maxDelaySeconds : Long, awaitStabilizationSeconds : Long )
case class Config( dbName : String, jdbcUrl : String, dumpDir : os.Path, feeds : Set[Config.Feed] )








