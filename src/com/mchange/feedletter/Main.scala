package com.mchange.feedletter

import zio.*
import zio.cli.{CliApp,Command,ZIOCliDefault}
import zio.cli.HelpDoc.Span.text

object Main extends ZIOCliDefault:
  val config = com.mchange.feedletter.UserConfig

  val mainCommand = Command("feedletter")

  val cliApp = CliApp.make(
    name = "feedletter",
    version = com.mchange.feedletter.BuildInfo.version,
    summary = text("Manage e-mail subscriptions to RSS feeds."),
    command = mainCommand
  )(
    _ => Console.printLine("Hello.")
  )
