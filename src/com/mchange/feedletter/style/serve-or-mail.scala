package com.mchange.feedletter.style

import zio.*

import java.time.ZoneId

import com.mchange.feedletter.*
import com.mchange.feedletter.api.ApiLinkGenerator

import com.mchange.mailutil.Smtp

import com.mchange.conveniences.collection.*
import com.mchange.feedletter.db.PgDatabase.subscriptionsForSubscribableName

object DummyApiLinkGenerator extends ApiLinkGenerator:
  def createGetLink( subscribableName : SubscribableName, destination : Destination ) : String =
    s"http://localhost:8024/v0/subscription/create?subscribableName=${subscribableName}&destinationType=Email&addressPart=fakeuser%40example.com"
  def confirmGetLink( sid : SubscriptionId ) : String =
    s"http://localhost:8024/v0/subscription/confirm?subscriptionId=${sid}&invitation=fake"
  def removeGetLink( sid : SubscriptionId ) : String =
    s"http://localhost:8024/v0/subscription/remove?subscriptionId=${sid}&invitation=fake"

def serveOneHtmlPage( html : String, interface : String, port : Int ) : Task[Unit] =
  import zio.http.Server
  import sttp.tapir.ztapir.*
  import sttp.tapir.server.ziohttp.ZioHttpInterpreter

  val rootEndpoint = endpoint.get.out( htmlBodyUtf8 )
  val indexEndpoint = endpoint.in("index.html").get.out( htmlBodyUtf8 )
  val logic : Unit => UIO[String] = _ => ZIO.succeed( html )
  val httpApp = ZioHttpInterpreter().toHttp( List(rootEndpoint.zServerLogic(logic), indexEndpoint.zServerLogic(logic) ) )
  for
    _ <- Console.printLine( s"Starting single-page webserver on interface '${interface}', port ${port}..." )
    _ <- Server.serve(httpApp).provide(ZLayer.succeed(Server.Config.default.binding(interface,port)), Server.live)
    _ <- ZIO.never
  yield ()


def serveOrMailStatusChangeUntemplate(
  untemplateName       : String,
  statusChange         : SubscriptionStatusChange,
  subscribableName     : SubscribableName,
  subscriptionManager  : SubscriptionManager,
  destination          : subscriptionManager.D,
  requiresConfirmation : Boolean,
  styleDest            : StyleDest,
  appSetup             : AppSetup
) : Task[Unit] =
  styleDest match
    case StyleDest.Serve( interface, port ) =>
      serveStatusChangeUntemplate( untemplateName, statusChange, subscribableName, subscriptionManager, destination, requiresConfirmation, interface, port )
    case StyleDest.Mail( from, to ) =>
      mailStatusChangeUntemplate( untemplateName, statusChange, subscribableName, subscriptionManager, destination, requiresConfirmation, from, to, appSetup.smtpContext )

def serveStatusChangeUntemplate(
  untemplateName       : String,
  statusChange         : SubscriptionStatusChange,
  subscribableName     : SubscribableName,
  subscriptionManager  : SubscriptionManager,
  destination          : subscriptionManager.D,
  requiresConfirmation : Boolean,
  interface            : String,
  port                 : Int
) : Task[Unit] =
  val filled = fillStatusChangeTemplate( untemplateName, statusChange, subscribableName, subscriptionManager, destination, requiresConfirmation )
  serveOneHtmlPage( filled, interface, port )

def mailStatusChangeUntemplate(
  untemplateName       : String,
  statusChange         : SubscriptionStatusChange,
  subscribableName     : SubscribableName,
  subscriptionManager  : SubscriptionManager,
  destination          : subscriptionManager.D,
  requiresConfirmation : Boolean,
  from                 : Smtp.Address,
  to                   : Smtp.Address,
  smtpContext          : Smtp.Context
) : Task[Unit] =
  ZIO.attempt:
    val filled = fillStatusChangeTemplate( untemplateName, statusChange, subscribableName, subscriptionManager, destination, requiresConfirmation )
    given ctx : Smtp.Context = smtpContext
    Smtp.sendSimpleHtmlOnly(filled, from = from, to = to, subject = s"[${subscribableName}] Example Status Change")
    println( "Mail sent." )

def serveOrMailComposeMultipleUntemplate(
  untemplateName      : String,
  subscribableName    : SubscribableName,
  subscriptionManager : SubscriptionManager,
  withinTypeId        : String,
  destination         : subscriptionManager.D,
  timeZone            : ZoneId,
  feedUrl             : FeedUrl,
  digest              : FeedDigest,
  guids               : Seq[Guid],
  styleDest           : StyleDest,
  appSetup            : AppSetup
) : Task[Unit] =
  styleDest match
    case StyleDest.Serve( interface, port ) =>
      serveComposeMultipleUntemplate( untemplateName, subscribableName, subscriptionManager, withinTypeId, destination, timeZone, feedUrl, digest, guids, interface, port )
    case StyleDest.Mail( from, to ) =>
      mailComposeMultipleUntemplate( untemplateName, subscribableName, subscriptionManager, withinTypeId, destination, timeZone, feedUrl, digest, guids, from, to, appSetup.smtpContext )

def serveComposeMultipleUntemplate(
  untemplateName      : String,
  subscribableName    : SubscribableName,
  subscriptionManager : SubscriptionManager,
  withinTypeId        : String,
  destination         : subscriptionManager.D,
  timeZone            : ZoneId,
  feedUrl             : FeedUrl,
  digest              : FeedDigest,
  guids               : Seq[Guid],
  interface           : String,
  port                : Int
) : Task[Unit] =
  val composed = fillComposeMultipleUntemplate(untemplateName,subscribableName,subscriptionManager,withinTypeId,destination,timeZone,feedUrl,digest,guids)
  composed match
    case Some(c) => serveOneHtmlPage( c, interface, port )
    case None    => Console.printLine(s"""After customization, perhaps also before, there were no contents to display. Original guids: ${digest.fileOrderedGuids.mkString(", ")}""")

def mailComposeMultipleUntemplate(
  untemplateName      : String,
  subscribableName    : SubscribableName,
  subscriptionManager : SubscriptionManager,
  withinTypeId        : String,
  destination         : subscriptionManager.D,
  timeZone            : ZoneId,
  feedUrl             : FeedUrl,
  digest              : FeedDigest,
  guids               : Seq[Guid],
  from                : Smtp.Address,
  to                  : Smtp.Address,
  smtpContext         : Smtp.Context
) : Task[Unit] =
  val composed = fillComposeMultipleUntemplate(untemplateName,subscribableName,subscriptionManager,withinTypeId,destination,timeZone,feedUrl,digest,guids)
  composed match
    case Some(c) =>
      val contents = guids.map( digest.guidToItemContent.get ).flatten
      val subject =
        subscriptionManager match
          case esm : SubscriptionManager.Email => "EXAMPLE: " + esm.subject( subscribableName, withinTypeId, feedUrl, contents, timeZone )
          case _ => s"[${subscribableName}] Example multiple-item notification"
      ZIO.attempt:
        given ctx : Smtp.Context = smtpContext
        Smtp.sendSimpleHtmlOnly(c, from = from, to = to, subject = subject)
        println( "Mail sent." )
    case None =>
      Console.printLine(s"""After customization, perhaps also before, there were no contents to display. Original guids: ${digest.fileOrderedGuids.mkString(", ")}""")

def serveOrMailComposeSingleUntemplate(
  untemplateName      : String,
  subscribableName    : SubscribableName,
  subscriptionManager : SubscriptionManager,
  withinTypeId        : String,
  destination         : subscriptionManager.D,
  timeZone            : ZoneId,
  feedUrl             : FeedUrl,
  digest              : FeedDigest,
  guid                : Guid,
  styleDest           : StyleDest,
  appSetup            : AppSetup
) : Task[Unit] =
  styleDest match
    case StyleDest.Serve( interface, port ) =>
      serveComposeSingleUntemplate( untemplateName, subscribableName, subscriptionManager, withinTypeId, destination, timeZone, feedUrl, digest, guid, interface, port )
    case StyleDest.Mail( from, to ) =>
      mailComposeSingleUntemplate( untemplateName, subscribableName, subscriptionManager, withinTypeId, destination, timeZone, feedUrl, digest, guid, from, to, appSetup.smtpContext )

def serveComposeSingleUntemplate(
  untemplateName      : String,
  subscribableName    : SubscribableName,
  subscriptionManager : SubscriptionManager,
  withinTypeId        : String,
  destination         : subscriptionManager.D,
  timeZone            : ZoneId,
  feedUrl             : FeedUrl,
  digest              : FeedDigest,
  guid                : Guid,
  interface           : String,
  port                : Int
) : Task[Unit] =
  val composed = fillComposeSingleUntemplate(untemplateName,subscribableName,subscriptionManager,withinTypeId,destination,timeZone,feedUrl,digest,guid)
  composed match
    case Some(c) => serveOneHtmlPage( c, interface, port )
    case None    => Console.printLine(s"After customization, there were no contents to display. Original contents: ${digest.guidToItemContent( guid )}")

def mailComposeSingleUntemplate(
  untemplateName      : String,
  subscribableName    : SubscribableName,
  subscriptionManager : SubscriptionManager,
  withinTypeId        : String,
  destination         : subscriptionManager.D,
  timeZone            : ZoneId,
  feedUrl             : FeedUrl,
  digest              : FeedDigest,
  guid                : Guid,
  from                : Smtp.Address,
  to                  : Smtp.Address,
  smtpContext         : Smtp.Context
) : Task[Unit] =
  val composed = fillComposeSingleUntemplate(untemplateName,subscribableName,subscriptionManager,withinTypeId,destination,timeZone,feedUrl,digest,guid)
  composed match
    case Some(c) =>
      val contents = digest.guidToItemContent( guid )
      val subject =
        subscriptionManager match
          case esm : SubscriptionManager.Email => "EXAMPLE: " + esm.subject( subscribableName, withinTypeId, feedUrl, Seq(contents), timeZone )
          case _ => s"[${subscribableName}] Example single-item notification"
      ZIO.attempt:
        given ctx : Smtp.Context = smtpContext
        Smtp.sendSimpleHtmlOnly(c, from = from, to = to, subject = subject)
        println( "Mail sent." )
    case None =>
      Console.printLine(s"After customization, there were no contents to display. Original contents: ${digest.guidToItemContent( guid )}")
def serveOrMailConfirmUntemplate(
  untemplateName      : String,
  subscribableName    : SubscribableName,
  subscriptionManager : SubscriptionManager,
  destination         : subscriptionManager.D,
  feedUrl             : FeedUrl,
  confirmHours        : Int,
  styleDest           : StyleDest,
  as                  : AppSetup
) : Task[Unit] =
  styleDest match
    case StyleDest.Serve( interface, port ) =>
      serveConfirmUntemplate( untemplateName, subscribableName, subscriptionManager, destination, feedUrl, confirmHours, interface, port )
    case StyleDest.Mail( from, to ) =>
      mailConfirmUntemplate( untemplateName, subscribableName, subscriptionManager, destination, feedUrl, confirmHours, from, to, as.smtpContext )

def serveConfirmUntemplate(
  untemplateName      : String,
  subscribableName    : SubscribableName,
  subscriptionManager : SubscriptionManager,
  destination         : subscriptionManager.D,
  feedUrl             : FeedUrl,
  confirmHours        : Int,
  interface           : String,
  port                : Int
) : Task[Unit] =
  val filled = fillConfirmUntemplate(untemplateName,subscribableName,subscriptionManager,destination,feedUrl,confirmHours)
  serveOneHtmlPage( filled, interface, port )

def mailConfirmUntemplate(
  untemplateName      : String,
  subscribableName    : SubscribableName,
  subscriptionManager : SubscriptionManager,
  destination         : subscriptionManager.D,
  feedUrl             : FeedUrl,
  confirmHours        : Int,
  from                : Smtp.Address,
  to                  : Smtp.Address,
  smtpContext         : Smtp.Context
) : Task[Unit] =
  ZIO.attempt:
    val filled = fillConfirmUntemplate(untemplateName,subscribableName,subscriptionManager,destination,feedUrl,confirmHours)
    given ctx : Smtp.Context = smtpContext
    Smtp.sendSimpleHtmlOnly(filled, from = from, to = to, subject = s"[${subscribableName}] Example Confirm")
    println( "Mail sent." )

def serveOrMailRemovalNotificationUntemplate(
  untemplateName      : String,
  subscribableName    : SubscribableName,
  subscriptionManager : SubscriptionManager,
  destination         : subscriptionManager.D,
  styleDest           : StyleDest,
  appSetup            : AppSetup
) : Task[Unit] =
  styleDest match
    case StyleDest.Serve( interface, port ) =>
      serveRemovalNotificationUntemplate( untemplateName, subscribableName, subscriptionManager, destination, interface, port )
    case StyleDest.Mail( from, to ) =>
      mailRemovalNotificationUntemplate( untemplateName, subscribableName, subscriptionManager, destination, from, to, appSetup.smtpContext )

def serveRemovalNotificationUntemplate(
  untemplateName      : String,
  subscribableName    : SubscribableName,
  subscriptionManager : SubscriptionManager,
  destination         : subscriptionManager.D,
  interface           : String,
  port                : Int
) : Task[Unit] =
  val filled = fillRemovalNotificationUntemplate(untemplateName,subscribableName,subscriptionManager,destination)
  serveOneHtmlPage( filled, interface, port )

def mailRemovalNotificationUntemplate(
  untemplateName      : String,
  subscribableName    : SubscribableName,
  subscriptionManager : SubscriptionManager,
  destination         : subscriptionManager.D,
  from                : Smtp.Address,
  to                  : Smtp.Address,
  smtpContext         : Smtp.Context
) : Task[Unit] =
  ZIO.attempt:
    val filled = fillRemovalNotificationUntemplate(untemplateName,subscribableName,subscriptionManager,destination)
    given ctx : Smtp.Context = smtpContext
    Smtp.sendSimpleHtmlOnly(filled, from = from, to = to, subject = s"[${subscribableName}] Example Removal Notification")
    println( "Mail sent." )

private def fillStatusChangeTemplate(
  untemplateName       : String,
  statusChange         : SubscriptionStatusChange,
  subscribableName     : SubscribableName,
  subscriptionManager  : SubscriptionManager,
  destination          : subscriptionManager.D,
  requiresConfirmation : Boolean
) : String =
  val unsubscribeLink = DummyApiLinkGenerator.removeGetLink(SubscriptionId(0))
  val resubscribeLink = DummyApiLinkGenerator.createGetLink(subscribableName,destination)
  val sci = StatusChangeInfo( statusChange, subscribableName, subscriptionManager, destination, requiresConfirmation, unsubscribeLink, resubscribeLink )
  val untemplate = AllUntemplates.findStatusChangeUntemplate( untemplateName )
  untemplate( sci ).text

private def fillComposeMultipleUntemplate(
  untemplateName      : String,
  subscribableName    : SubscribableName,
  subscriptionManager : SubscriptionManager,
  withinTypeId        : String,
  destination         : subscriptionManager.D,
  timeZone            : ZoneId,
  feedUrl             : FeedUrl,
  digest              : FeedDigest,
  guids               : Seq[Guid]
) : Option[String] =
  val contents = guids.map( digest.guidToItemContent.get ).collect { case Some(content) => content }
  val customizedContents = subscriptionManager.customizeContents( subscribableName, withinTypeId, feedUrl, contents, timeZone )
  if customizedContents.nonEmpty then
    val composeInfo = ComposeInfo.Multiple( feedUrl, subscribableName, subscriptionManager, withinTypeId, timeZone, customizedContents )
    val untemplate = AllUntemplates.findComposeUntemplateMultiple( untemplateName )
    val composed =
      val untemplateOutput = untemplate( composeInfo ).text
      val sid = SubscriptionId(0)
      val templateParams = subscriptionManager.composeTemplateParams( subscribableName, withinTypeId, feedUrl, destination, sid, DummyApiLinkGenerator.removeGetLink(sid) )
      templateParams.fill( untemplateOutput )
    Some(composed)
  else
    None

private def fillComposeSingleUntemplate(
  untemplateName      : String,
  subscribableName    : SubscribableName,
  subscriptionManager : SubscriptionManager,
  withinTypeId        : String,
  destination         : subscriptionManager.D,
  timeZone            : ZoneId,
  feedUrl             : FeedUrl,
  digest              : FeedDigest,
  guid                : Guid
) : Option[String] =
  val contents = digest.guidToItemContent( guid )
  val customizedContents = subscriptionManager.customizeContents( subscribableName, withinTypeId, feedUrl, Seq(contents), timeZone )
  if customizedContents.nonEmpty then
    val uniqueContent = customizedContents.uniqueOr: (c, nu) =>
      throw new WrongContentsMultiplicity(s"${this}: We expect exactly one item to render, found $nu: " + customizedContents.map( ci => (ci.title orElse ci.link).getOrElse("<item>") ).mkString(", "))
    val composeInfo = ComposeInfo.Single( feedUrl, subscribableName, subscriptionManager, withinTypeId, timeZone, uniqueContent )
    val untemplate = AllUntemplates.findComposeUntemplateSingle( untemplateName )
    val composed =
      val untemplateOutput = untemplate( composeInfo ).text
      val sid = SubscriptionId(0)
      val templateParams = subscriptionManager.composeTemplateParams( subscribableName, withinTypeId, feedUrl, destination, sid, DummyApiLinkGenerator.removeGetLink(sid) )
      templateParams.fill( untemplateOutput )
    Some(composed)
  else
    None

private def fillConfirmUntemplate(
  untemplateName      : String,
  subscribableName    : SubscribableName,
  subscriptionManager : SubscriptionManager,
  destination         : subscriptionManager.D,
  feedUrl             : FeedUrl,
  confirmHours        : Int
) : String =
  val sid = SubscriptionId(0)
  val confirmInfo = ConfirmInfo( destination, subscribableName, subscriptionManager, DummyApiLinkGenerator.confirmGetLink(sid), DummyApiLinkGenerator.removeGetLink(sid), confirmHours )
  val untemplate = AllUntemplates.findConfirmUntemplate( untemplateName )
  untemplate( confirmInfo ).text

private def fillRemovalNotificationUntemplate(
  untemplateName      : String,
  subscribableName    : SubscribableName,
  subscriptionManager : SubscriptionManager,
  destination         : subscriptionManager.D
) : String =
  val sid = SubscriptionId(0)
  val rnInfo = RemovalNotificationInfo( subscribableName, subscriptionManager, destination, DummyApiLinkGenerator.createGetLink(subscribableName,destination))
  val untemplate = AllUntemplates.findRemovalNotificationUntemplate( untemplateName )
  untemplate( rnInfo ).text


