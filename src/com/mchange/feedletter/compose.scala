package com.mchange.feedletter

import zio.*

import untemplate.Untemplate

import db.AssignableKey

object ComposeInfo:
  sealed trait Universal:
    def feedUrl             : String
    def subscriptionName    : String
    def subscriptionManager : SubscriptionManager
    def withinTypeId        : String
    def contents            : ItemContent | Set[ItemContent]
  end Universal
  case class Single( feedUrl : String, subscriptionName : String, subscriptionManager: SubscriptionManager, withinTypeId : String, contents : ItemContent ) extends ComposeInfo.Universal
  case class Multiple( feedUrl : String, subscriptionName : String, subscriptionManager: SubscriptionManager, withinTypeId : String, contents : Set[ItemContent] ) extends ComposeInfo.Universal

object ComposeSelection:
  object Single:
    case object First extends Single
    case object Random extends Single
    case class Guid( guid : com.mchange.feedletter.Guid ) extends Single
  sealed trait Single
  //object Multiple:
  //sealed trait Multiple

def composeMultipleItemHtmlMailTemplate( assignableKey : AssignableKey, stype : SubscriptionManager, contents : Set[ItemContent] ) : String = ???

// def composeSingleItemHtmlMailTemplate( assignableKey : AssignableKey, stype : SubscriptionManager, contents : ItemContent ) : String = ???

def serveComposeSingleUntemplate(
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
        val templateParams = templating.composeTemplateParams( subscriptionName, withinTypeId, feedUrl, d, Set(contents) )
        templateParams.fill( untemplateOutput )
   // case _ => // this case will become relavant when some non-templating SubscriptionManagers are defined
   //   untemplateOutput 
  serveOneHtmlPage( composed, port )
