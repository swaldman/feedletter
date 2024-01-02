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

