package com.mchange.feedletter

object Default:
  object Feed:
    val MinDelayMinutes           = 30
    val AwaitStabilizationMinutes = 15
    val MaxDelayMinutes           = 180
    val RecheckEveryMinutes       = 10

  object Style:
    val StyleInterface = "0.0.0.0"
    val StylePort      = 8080

  object Config:
    val ConfirmHours          : Int         = 48
    val MailBatchSize         : Int         = 100
    val MailBatchDelaySeconds : Int         = 10 * 60 // 10 mins
    val MailMaxRetries        : Int         = 5
    val WebDaemonPort         : Int         = 8024
    val WebDaemonInterface    : String      = "127.0.0.1" // use IPv4 or IPv6, anything InetAddress.getByName(...) can interpret
    val WebApiProtocol        : String      = "http"
    val WebApiHostName        : String      = "localhost"
    val WebApiBasePath        : String      = "/"
    val WebApiPort            : Option[Int] = None

  object Email:
    val ComposeUntemplateSingle   = "com.mchange.feedletter.default.email.composeUniversal"
    val ComposeUntemplateMultiple = "com.mchange.feedletter.default.email.composeUniversal"
    val ConfirmUntemplate         = "com.mchange.feedletter.default.email.confirm"
    val StatusChangeUntemplate    = "com.mchange.feedletter.default.email.statusChange"

