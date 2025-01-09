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
    val BlueskyMaxRetries     : Int         = 10
    val ConfirmHours          : Int         = 48
    val MailBatchSize         : Int         = 100
    val MailBatchDelaySeconds : Int         = 10 * 60 // 10 mins
    val MailMaxRetries        : Int         = 5
    val MastodonMaxRetries    : Int         = 10
    val WebDaemonPort         : Int         = 8024
    val WebDaemonInterface    : String      = "127.0.0.1" // use IPv4 or IPv6, anything InetAddress.getByName(...) can interpret
    val WebApiProtocol        : String      = "http"
    val WebApiHostName        : String      = "localhost"
    val WebApiBasePath        : String      = "/"
    val WebApiPort            : Option[Int] = None

  object Email:
    val ComposeUntemplateSingle       = "com.mchange.feedletter.default.email.composeUniversal_html"
    val ComposeUntemplateMultiple     = "com.mchange.feedletter.default.email.composeUniversal_html"
    val ConfirmUntemplate             = "com.mchange.feedletter.default.email.confirm_html"
    val StatusChangeUntemplate        = "com.mchange.feedletter.default.email.statusChange_html"
    val RemovalNotificationUntemplate = "com.mchange.feedletter.default.email.removalNotification_html"

