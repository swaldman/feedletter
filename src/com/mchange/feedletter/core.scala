package com.mchange.feedletter

import zio.*
import javax.sql.DataSource
import com.mchange.feedletter.Config

type ZCommand = ZIO[AppSetup & Config & DataSource, Throwable, Any]











