package com.mchange.feedletter

import com.mchange.feedletter.style.AllUntemplates

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
    object LocalName:
       val Prefix = "local.subscription.email."
       val ComposeUntemplateSingle       = Prefix + "composeSingle_html"
       val ComposeUntemplateMultiple     = Prefix + "composeMultiple_html"
       val ComposeUntemplateUniversal    = Prefix + "composeUniversal_html"
       val ConfirmUntemplate             = Prefix + "confirm_html"
       val StatusChangeUntemplate        = Prefix + "statusChange_html"
       val RemovalNotificationUntemplate = Prefix + "removalNotification_html"
    object BuiltIn:
       val ComposeUntemplateSingle       = "com.mchange.feedletter.default.email.composeUniversal_html"
       val ComposeUntemplateMultiple     = "com.mchange.feedletter.default.email.composeUniversal_html"
       val ConfirmUntemplate             = "com.mchange.feedletter.default.email.confirm_html"
       val StatusChangeUntemplate        = "com.mchange.feedletter.default.email.statusChange_html"
       val RemovalNotificationUntemplate = "com.mchange.feedletter.default.email.removalNotification_html"


    private def isDefined( fqn : String ) : Boolean = AllUntemplates.isDefined(fqn)

    def composeUntemplateSingle() : String =
      if isDefined( LocalName.ComposeUntemplateSingle ) then LocalName.ComposeUntemplateSingle
      else if isDefined( LocalName.ComposeUntemplateUniversal ) then LocalName.ComposeUntemplateUniversal
      else BuiltIn.ComposeUntemplateSingle

    def composeUntemplateMultiple() : String =
      if isDefined( LocalName.ComposeUntemplateMultiple ) then LocalName.ComposeUntemplateMultiple
      else if isDefined( LocalName.ComposeUntemplateUniversal ) then LocalName.ComposeUntemplateUniversal
      else BuiltIn.ComposeUntemplateMultiple

    def confirmUntemplate() : String =
      if isDefined( LocalName.ConfirmUntemplate ) then LocalName.ConfirmUntemplate
      else BuiltIn.ConfirmUntemplate

    def statusChangeUntemplate() : String =
      if isDefined( LocalName.StatusChangeUntemplate ) then LocalName.StatusChangeUntemplate
      else BuiltIn.StatusChangeUntemplate

    def removalNotificationUntemplate() : String =
      if isDefined( LocalName.RemovalNotificationUntemplate ) then LocalName.RemovalNotificationUntemplate
      else BuiltIn.RemovalNotificationUntemplate


