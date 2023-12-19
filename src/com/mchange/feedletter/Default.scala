package com.mchange.feedletter

object Default:
  val MinDelayMinutes           = 30
  val AwaitStabilizationMinutes = 15
  val MaxDelayMinutes           = 180
  val MailBatchSize             = 100
  val MailBatchDelaySeconds     = 15 * 60 // 15 mins
  val MailMaxRetries            = 5
  val ComposePort               = 8080
