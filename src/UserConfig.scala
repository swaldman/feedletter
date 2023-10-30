package com.mchange.feedletter

import com.mchange.v2.c3p0.ComboPooledDataSource

val UserConfig =
  Config(
    dataSource = new ComboPooledDataSource(),
    dbName = "feedletter",
    dumpDir = os.Path("/Users/swaldman/tmp/feedletter-db-dumps"),
    feeds = Set.empty
  )
