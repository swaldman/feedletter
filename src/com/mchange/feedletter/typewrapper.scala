package com.mchange.feedletter

object FeedId:
  def apply( i : Int ) : FeedId = i
opaque type FeedId = Int

extension( feedId : FeedId )
  def toInt : Int = feedId

object FeedUrl:
  def apply( s : String ) : FeedUrl = s
opaque type FeedUrl = String

object Guid:
  def apply( s : String ) : Guid = s
opaque type Guid = String

object SubscribableName:
  def apply( s : String ) : SubscribableName = s
opaque type SubscribableName = String

object SubscriptionId:
  def apply( l : Long ) : SubscriptionId = l
opaque type SubscriptionId = Long

extension (sid : SubscriptionId)
  def toLong : Long = sid

opaque type Json = String

object SubscriptionManagerJson:
  def apply( s : String ) : SubscriptionManagerJson = s
opaque type SubscriptionManagerJson <: Json = String

object DestinationJson:
  def apply( s : String ) : DestinationJson = s
opaque type DestinationJson <: Json = String


