package com.mchange.feedletter

import com.mchange.mailutil.Smtp
import scala.annotation.targetName

sealed trait AddressHeaderType
opaque type To      <: AddressHeaderType = Nothing
opaque type From    <: AddressHeaderType = Nothing
opaque type ReplyTo <: AddressHeaderType = Nothing

object AddressHeader:
  def apply[T <: AddressHeaderType]( s : String )                      : AddressHeader[T] = s
  def apply[T <: AddressHeaderType]( address : Smtp.Address )          : AddressHeader[T] = address.rendered
  def apply[T <: AddressHeaderType]( destination : Destination.Email ) : AddressHeader[T] = destination.rendered
opaque type AddressHeader[T <: AddressHeaderType] = String

extension[T <: AddressHeaderType] ( addressHeader : AddressHeader[T] )
  @targetName("addressHeaderToString") inline def str : String = addressHeader

object BskyEntrywayUrl:
  def apply( s : String ) : BskyEntrywayUrl = s
opaque type BskyEntrywayUrl = String

extension( bskyEntrywayUrl : BskyEntrywayUrl )
  @targetName("bskyEntrywayUrlToString") inline def str : String = bskyEntrywayUrl

object BskyIdentifier:
  def apply( s : String ) : BskyIdentifier = s
opaque type BskyIdentifier = String

extension( bskyIdentifier : BskyIdentifier )
  @targetName("bskyIdentifierToString") inline def str : String = bskyIdentifier

object BskyPostableId:
  def apply( l : Long ) : BskyPostableId = l
opaque type BskyPostableId = Long

extension ( bspid : BskyPostableId )
  @targetName("bskyPostableIdToLong") inline def toLong : Long = bspid

object DestinationJson:
  def apply( s : String ) : DestinationJson = s
opaque type DestinationJson <: Json = String

extension( destinationJson : DestinationJson )
  @targetName("destinationJsonToString") inline def str : String = destinationJson

object FeedId:
  def apply( i : Int ) : FeedId = i
opaque type FeedId = Int

extension( feedId : FeedId )
  @targetName("feedIdToInt") inline def toInt : Int = feedId

object FeedUrl:
  def apply( s : String ) : FeedUrl = s
opaque type FeedUrl = String

extension( feedUrl : FeedUrl )
  @targetName("feedUrlToString") inline def str : String = feedUrl

object Guid:
  def apply( s : String ) : Guid = s
opaque type Guid = String

extension( guid : Guid )
  @targetName("guidToString") inline def str : String = guid

object SubscribableName:
  def apply( s : String ) : SubscribableName = s
opaque type SubscribableName = String

extension( subscribableName : SubscribableName )
  @targetName("subscribableNameToString") inline def str : String = subscribableName

object SubscriptionId:
  def apply( l : Long ) : SubscriptionId = l
opaque type SubscriptionId = Long

extension (sid : SubscriptionId)
  @targetName("subscriptionIdToLong") inline def toLong : Long = sid

opaque type Json = String

object SubscriptionManagerJson:
  def apply( s : String ) : SubscriptionManagerJson = s
opaque type SubscriptionManagerJson <: Json = String

extension( subscriptionManagerJson : SubscriptionManagerJson )
  @targetName("subscriptionManagerJsonToString") inline def str : String = subscriptionManagerJson

object MastoName:
  def apply( s : String ) : MastoName = s
opaque type MastoName = String

extension( mastoName : MastoName )
  @targetName("mastoNameToString") inline def str : String = mastoName

object MastoInstanceUrl:
  def apply( s : String ) : MastoInstanceUrl = s
opaque type MastoInstanceUrl = String

extension( mastoInstanceUrl : MastoInstanceUrl )
  @targetName("mastoInstanceUrlToString") inline def str : String = mastoInstanceUrl

object MastoPostableId:
  def apply( l : Long ) : MastoPostableId = l
opaque type MastoPostableId = Long

extension ( mpid : MastoPostableId )
  @targetName("mastoPostableIdToLong") inline def toLong : Long = mpid

