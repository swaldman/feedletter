package com.mchange.feedletter

object Default:
  val MinDelayMinutes           = 30
  val AwaitStabilizationMinutes = 15
  val MaxDelayMinutes           = 180
  val RecheckEveryMinutes       = 10
  val MailBatchSize             = 100
  val MailBatchDelaySeconds     = 15 * 60 // 15 mins
  val MailMaxRetries            = 5
  val StylePort               = 8080

  object Email:
    val ComposeUntemplateSingle          = "com.mchange.feedletter.default.email.composeSingle"
    val ComposeUntemplateMultiple        = "com.mchange.feedletter.default.email.composeMultiple"
    val ConfirmUntemplate                = "com.mchange.feedletter.default.email.confirm"
    val StatusChangedUntemplate          = "com.mchange.feedletter.default.email.statusChanged"

