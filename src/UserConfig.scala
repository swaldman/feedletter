package com.mchange.feedletter

val UserConfig =
  Config(
    dbName = "feedletter",
    jdbcUrl = "jdbc:postgresql://localhost:5432/feedletter",
    dumpDir = os.Path("/Users/swaldman/tmp/feedletter-db-dumps"),
    feeds = Set.empty
  )
