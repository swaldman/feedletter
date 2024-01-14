package com.mchange.feedletter.style

import zio.*

import java.time.ZoneId

import com.mchange.feedletter.*
import com.mchange.feedletter.api.ApiLinkGenerator

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
  Server.serve(httpApp).provide(ZLayer.succeed(Server.Config.default.binding(interface,port)), Server.live)

def styleStatusChangeUntemplate(
  untemplateName       : String,
  statusChange         : SubscriptionStatusChange,
  subscribableName     : SubscribableName,
  subscriptionManager  : SubscriptionManager,
  destination          : subscriptionManager.D,
  requiresConfirmation : Boolean,
  interface            : String,
  port                 : Int
) : Task[Unit] =
  val unsubscribeLink = DummyApiLinkGenerator.removeGetLink(SubscriptionId(0))
  val resubscribeLink = DummyApiLinkGenerator.createGetLink(subscribableName,destination)
  val sci = StatusChangeInfo( statusChange, subscribableName, subscriptionManager, destination, requiresConfirmation, unsubscribeLink, resubscribeLink )
  val untemplate = AllUntemplates.findStatusChangeUntemplate( untemplateName )
  val filled = untemplate( sci ).text
  serveOneHtmlPage( filled, interface, port )

def styleComposeMultipleUntemplate(
  untemplateName      : String,
  subscribableName    : SubscribableName,
  subscriptionManager : SubscriptionManager,
  withinTypeId        : String,
  destination         : subscriptionManager.D,
  timeZone            : ZoneId,
  feedUrl             : FeedUrl,
  digest              : FeedDigest,
  guids               : Set[Guid],
  interface           : String,
  port                : Int
) : Task[Unit] =
  val contents = guids.map( digest.guidToItemContent.get ).collect { case Some(content) => content }
  val composeInfo = ComposeInfo.Multiple( feedUrl, subscribableName, subscriptionManager, withinTypeId, timeZone,  contents )
  val untemplate = AllUntemplates.findComposeUntemplateMultiple( untemplateName )
  val composed =
    val untemplateOutput = untemplate( composeInfo ).text
    val sid = SubscriptionId(0)
    val templateParams = subscriptionManager.composeTemplateParams( subscribableName, withinTypeId, feedUrl, destination, sid, DummyApiLinkGenerator.removeGetLink(sid) )
    templateParams.fill( untemplateOutput )
  serveOneHtmlPage( composed, interface, port )

def styleComposeSingleUntemplate(
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
  val contents = digest.guidToItemContent( guid )
  val composeInfo = ComposeInfo.Single( feedUrl, subscribableName, subscriptionManager, withinTypeId, timeZone, contents )
  val untemplate = AllUntemplates.findComposeUntemplateSingle( untemplateName )
  val composed =
    val untemplateOutput = untemplate( composeInfo ).text
    val sid = SubscriptionId(0)
    val templateParams = subscriptionManager.composeTemplateParams( subscribableName, withinTypeId, feedUrl, destination, sid, DummyApiLinkGenerator.removeGetLink(sid) )
    templateParams.fill( untemplateOutput )
  serveOneHtmlPage( composed, interface, port )

def styleConfirmUntemplate(
  untemplateName      : String,
  subscribableName    : SubscribableName,
  subscriptionManager : SubscriptionManager,
  destination         : subscriptionManager.D,
  feedUrl             : FeedUrl,
  confirmHours        : Int,
  interface           : String,
  port                : Int
) : Task[Unit] =
  val sid = SubscriptionId(0)
  val confirmInfo = ConfirmInfo( destination, subscribableName, subscriptionManager, DummyApiLinkGenerator.confirmGetLink(sid), confirmHours )
  val untemplate = AllUntemplates.findConfirmUntemplate( untemplateName )
  val filled = untemplate( confirmInfo ).text
  serveOneHtmlPage( filled, interface, port )

def styleRemovalNotificationUntemplate(
  untemplateName      : String,
  subscribableName    : SubscribableName,
  subscriptionManager : SubscriptionManager,
  destination         : subscriptionManager.D,
  interface           : String,
  port                : Int
) : Task[Unit] =
  val sid = SubscriptionId(0)
  val rnInfo = RemovalNotificationInfo( subscribableName, subscriptionManager, destination, DummyApiLinkGenerator.createGetLink(subscribableName,destination))
  val untemplate = AllUntemplates.findRemovalNotificationUntemplate( untemplateName )
  val filled = untemplate( rnInfo ).text
  serveOneHtmlPage( filled, interface, port )

