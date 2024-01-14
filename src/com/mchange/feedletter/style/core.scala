package com.mchange.feedletter.style

import com.mchange.feedletter.*

import java.time.ZoneId

import untemplate.Untemplate

def untemplateInputType( template : Untemplate.AnyUntemplate ) : String =
  template.UntemplateInputTypeCanonical.getOrElse( template.UntemplateInputTypeDeclared )

object ComposeInfo:
  sealed trait Universal:
    def feedUrl             : FeedUrl
    def subscribableName    : SubscribableName
    def subscriptionManager : SubscriptionManager
    def withinTypeId        : String
    def timeZone            : ZoneId
    def contents            : ItemContent | Set[ItemContent]
    def contentsSet         : Set[ItemContent]
  end Universal
  case class Single( feedUrl : FeedUrl, subscribableName : SubscribableName, subscriptionManager: SubscriptionManager, withinTypeId : String, timeZone : ZoneId, contents : ItemContent ) extends ComposeInfo.Universal:
    override lazy val contentsSet : Set[ItemContent] = Set( contents )
  case class Multiple( feedUrl : FeedUrl, subscribableName : SubscribableName, subscriptionManager: SubscriptionManager, withinTypeId : String, timeZone : ZoneId, contents : Set[ItemContent] ) extends ComposeInfo.Universal:
    override lazy val contentsSet : Set[ItemContent] = contents

object ComposeSelection:
  object Single:
    case object First extends Single
    case object Random extends Single
    case class Guid( guid : com.mchange.feedletter.Guid ) extends Single
  sealed trait Single
  object Multiple:
    case class First( n : Int ) extends Multiple
    case class Random( n : Int ) extends Multiple
    case class Guids( values : Set[Guid] ) extends Multiple
  sealed trait Multiple

case class ConfirmInfo( destination : Destination, subscribableName : SubscribableName, subscriptionManager : SubscriptionManager, confirmGetLink : String, confirmHours : Int )

case class RemovalNotificationInfo(
  subscribableName     : SubscribableName,
  subscriptionManager  : SubscriptionManager,
  destination          : Destination,
  resubscribeLink      : String
)

case class StatusChangeInfo(
  change               : SubscriptionStatusChange,
  subscribableName     : SubscribableName,
  subscriptionManager  : SubscriptionManager,
  destination          : Destination,
  requiresConfirmation : Boolean,
  unsubscribeLink      : String,
  resubscribeLink      : String
)

