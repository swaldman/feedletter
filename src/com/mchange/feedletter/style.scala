package com.mchange.feedletter

import zio.*
import com.mchange.feedletter.api.ApiLinkGenerator
import com.mchange.feedletter.Main.Admin.subscribe

object DummyApiLinkGenerator extends ApiLinkGenerator:
  def createGetLink( subscribableName : SubscribableName, destination : Destination ) : String =
    s"http://localhost:8024/v0/subscription/create?subscribableName=${subscribableName}&destinationType=Email&addressPart=fakeuser%40example.com"
  def confirmGetLink( sid : SubscriptionId ) : String =
    s"http://localhost:8024/v0/subscription/confirm?subscriptionId=${sid}&invitation=fake"
  def removeGetLink( sid : SubscriptionId ) : String =
    s"http://localhost:8024/v0/subscription/remove?subscriptionId=${sid}&invitation=fake"

def serveOneHtmlPage( html : String, port : Int ) : Task[Unit] =
  import zio.http.Server
  import sttp.tapir.ztapir.*
  import sttp.tapir.server.ziohttp.ZioHttpInterpreter

  val rootEndpoint = endpoint.get.out( htmlBodyUtf8 )
  val indexEndpoint = endpoint.in("index.html").get.out( htmlBodyUtf8 )
  val logic : Unit => UIO[String] = _ => ZIO.succeed( html )
  val httpApp = ZioHttpInterpreter().toHttp( List(rootEndpoint.zServerLogic(logic), indexEndpoint.zServerLogic(logic) ) )
  Server.serve(httpApp).provide(ZLayer.succeed(Server.Config.default.binding("0.0.0.0",port)), Server.live)

def styleStatusChangeUntemplate(
  untemplateName       : String,
  statusChange         : SubscriptionStatusChange,
  subscriptionName     : SubscribableName,
  subscriptionManager  : SubscriptionManager,
  destination          : subscriptionManager.D,
  requiresConfirmation : Boolean,
  port                 : Int
) : Task[Unit] =
  val unsubscribeLink = DummyApiLinkGenerator.removeGetLink(SubscriptionId(0))
  val resubscribeLink = DummyApiLinkGenerator.createGetLink(subscriptionName,destination)
  val sci = StatusChangeInfo( statusChange, subscriptionName.toString(), subscriptionManager, destination, requiresConfirmation, unsubscribeLink, resubscribeLink )
  val untemplate = AllUntemplates.findStatusChangeUntemplate( untemplateName )
  val filled = untemplate( sci ).text
  serveOneHtmlPage( filled, port )

def styleComposeMultipleUntemplate(
  untemplateName      : String,
  subscriptionName    : SubscribableName,
  subscriptionManager : SubscriptionManager,
  withinTypeId        : String,
  destination         : subscriptionManager.D,
  feedUrl             : FeedUrl,
  digest              : FeedDigest,
  guids               : Set[Guid],
  port                : Int
) : Task[Unit] =
  val contents = guids.map( digest.guidToItemContent.get ).collect { case Some(content) => content }
  val composeInfo = ComposeInfo.Multiple( feedUrl.toString(), subscriptionName.toString(), subscriptionManager, withinTypeId, contents )
  val untemplate = AllUntemplates.findComposeUntemplateMultiple( untemplateName )
  val composed =
    val untemplateOutput = untemplate( composeInfo ).text
    subscriptionManager match
      case templating : SubscriptionManager.TemplatingCompose =>
        val d : templating.D = destination.asInstanceOf[templating.D] // how can I let the compiler know templating == subscriptionManager?
        val sid = SubscriptionId(0)
        val templateParams = templating.composeTemplateParams( subscriptionName, withinTypeId, feedUrl, d, sid, DummyApiLinkGenerator.removeGetLink(sid) )
        templateParams.fill( untemplateOutput )
   // case _ => // this case will become relavant when some non-templating SubscriptionManagers are defined
   //   untemplateOutput 
  serveOneHtmlPage( composed, port )

def styleComposeSingleUntemplate(
  untemplateName      : String,
  subscriptionName    : SubscribableName,
  subscriptionManager : SubscriptionManager,
  withinTypeId        : String,
  destination         : subscriptionManager.D,
  feedUrl             : FeedUrl,
  digest              : FeedDigest,
  guid                : Guid,
  port                : Int
) : Task[Unit] =
  val contents = digest.guidToItemContent( guid )
  val composeInfo = ComposeInfo.Single( feedUrl.toString(), subscriptionName.toString(), subscriptionManager, withinTypeId, contents )
  val untemplate = AllUntemplates.findComposeUntemplateSingle( untemplateName )
  val composed =
    val untemplateOutput = untemplate( composeInfo ).text
    subscriptionManager match
      case templating : SubscriptionManager.TemplatingCompose =>
        val d : templating.D = destination.asInstanceOf[templating.D] // how can I let the compiler know templating == subscriptionManager?
        val sid = SubscriptionId(0)
        val templateParams = templating.composeTemplateParams( subscriptionName, withinTypeId, feedUrl, d, sid, DummyApiLinkGenerator.removeGetLink(sid) )
        templateParams.fill( untemplateOutput )
   // case _ => // this case will become relavant when some non-templating SubscriptionManagers are defined
   //   untemplateOutput 
  serveOneHtmlPage( composed, port )

def styleConfirmUntemplate(
  untemplateName      : String,
  subscriptionName    : SubscribableName,
  subscriptionManager : SubscriptionManager,
  destination         : subscriptionManager.D,
  feedUrl             : FeedUrl,
  confirmHours        : Int,
  port                : Int
) : Task[Unit] =
  val sid = SubscriptionId(0)
  val confirmInfo = ConfirmInfo( destination, subscriptionName, subscriptionManager, DummyApiLinkGenerator.confirmGetLink(sid), confirmHours )
  val untemplate = AllUntemplates.findConfirmUntemplate( untemplateName )
  val filled = untemplate( confirmInfo ).text
  serveOneHtmlPage( filled, port )

