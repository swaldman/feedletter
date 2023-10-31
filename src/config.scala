package com.mchange.feedletter.config

import zio.*
import javax.sql.DataSource
import com.mchange.v2.c3p0.ComboPooledDataSource
import com.mchange.feedletter.Config

def config : Config =
  Config(
    dbName = "feedletter",
    dumpDir = os.Path("/Users/swaldman/tmp/feedletter-db-dumps"),
    feeds = Set.empty
  )

def dataSource : DataSource =
  new ComboPooledDataSource()

