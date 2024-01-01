package com.mchange.feedletter

object Default:
  object Feed:
    val MinDelayMinutes           = 30
    val AwaitStabilizationMinutes = 15
    val MaxDelayMinutes           = 180
    val RecheckEveryMinutes       = 10

  object Style:
    val StylePort = 8080

  object Config:
    val MailBatchSize         = 100
    val MailBatchDelaySeconds = 15 * 60 // 15 mins
    val MailMaxRetries        = 5
    val WebDaemonPort         = 8024
    val WebDaemonInterface    = "127.0.0.1" // use IPv4 or IPv6, anything InetAddress.getByName(...) can interpret
    val WebApiBase            = "http://localhost/"

  object Email:
    val ComposeUntemplateSingle   = "com.mchange.feedletter.default.email.composeSingle"
    val ComposeUntemplateMultiple = "com.mchange.feedletter.default.email.composeMultiple"
    val ConfirmUntemplate         = "com.mchange.feedletter.default.email.confirm"
    val StatusChangedUntemplate   = "com.mchange.feedletter.default.email.statusChanged"

