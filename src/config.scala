package com.mchange.feedletter.config

import zio.*
import javax.sql.DataSource
import com.mchange.v2.c3p0.ComboPooledDataSource

def dataSource : DataSource =
  new ComboPooledDataSource()

