package com.mchange.feedletter

import zio.*
import com.mchange.feedletter.api.ApiLinkGenerator

def serveOneHtmlPage( html : String, port : Int ) : Task[Unit] =
  import zio.http.Server
  import sttp.tapir.ztapir.*
  import sttp.tapir.server.ziohttp.ZioHttpInterpreter

  val rootEndpoint = endpoint.get.out( htmlBodyUtf8 )
  val indexEndpoint = endpoint.in("index.html").get.out( htmlBodyUtf8 )
  val logic : Unit => UIO[String] = _ => ZIO.succeed( html )
  val httpApp = ZioHttpInterpreter().toHttp( List(rootEndpoint.zServerLogic(logic), indexEndpoint.zServerLogic(logic) ) )
  Server.serve(httpApp).provide(ZLayer.succeed(Server.Config.default.binding("0.0.0.0",port)), Server.live)

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
        val d : templating.D = destination.asInstanceOf[templating.D] // how can I let th compiler know templating == subscriptionManager?
        val sid = SubscriptionId(0)
        val templateParams = templating.composeTemplateParams( subscriptionName, withinTypeId, feedUrl, d, sid, ApiLinkGenerator.Dummy.removeGetLink(sid) )
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
  port                : Int
) : Task[Unit] =
  val sid = SubscriptionId(0)
  val confirmInfo = ConfirmInfo( destination, subscriptionName, subscriptionManager, ApiLinkGenerator.Dummy.confirmGetLink(sid) )
  val untemplate = AllUntemplates.findConfirmUntemplate( untemplateName )
  val filled = untemplate( confirmInfo ).text
  serveOneHtmlPage( filled, port )

