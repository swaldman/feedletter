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
    def contentsSet         : Set[ItemContent]
  end Universal
  case class Single( feedUrl : String, subscriptionName : String, subscriptionManager: SubscriptionManager, withinTypeId : String, contents : ItemContent ) extends ComposeInfo.Universal:
    override lazy val contentsSet : Set[ItemContent] = Set( contents )
  case class Multiple( feedUrl : String, subscriptionName : String, subscriptionManager: SubscriptionManager, withinTypeId : String, contents : Set[ItemContent] ) extends ComposeInfo.Universal:
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

