package com.mchange.feedletter

import com.mchange.feedletter.*

import scala.collection.mutable

import scala.annotation.targetName

import java.time.ZoneId

import MLevel.*

object Customizer extends SelfLogging:
  type Subject = ( subscribableName : SubscribableName, subscriptionManager : SubscriptionManager, feedUrl : FeedUrl, contents : Seq[ItemContent], timeZone : ZoneId ) => String
  type Contents = ( subscribableName : SubscribableName, subscriptionManager : SubscriptionManager, feedUrl : FeedUrl, contents : Seq[ItemContent], timeZone : ZoneId  ) => Seq[ItemContent]
  type MastoAnnouncement = ( subscribableName : SubscribableName, subscriptionManager : SubscriptionManager, feedUrl : FeedUrl, content : ItemContent, timeZone : ZoneId ) => Option[String]
  type TemplateParams =
    ( subscribableName : SubscribableName, subscriptionManager : SubscriptionManager, feedUrl : FeedUrl, destination : Destination, subscriptionId : SubscriptionId, removeLink : String ) => Map[String,String]
  type Filter = ( subscribableName : SubscribableName, subscriptionManager : SubscriptionManager, content : ItemContent ) => Boolean
  type HintAnnounceRestriction = ( subscribableName : SubscribableName, subscriptionManager : SubscriptionManager, content : ItemContent ) => Option[Iffy.HintAnnounce.Policy]

  object Subject                 extends Registry[Customizer.Subject]
  object Contents                extends Registry[Customizer.Contents]
  object MastoAnnouncement       extends Registry[Customizer.MastoAnnouncement]
  object TemplateParams          extends Registry[Customizer.TemplateParams]
  object Filter                  extends Registry[Customizer.Filter]

  object HintAnnounceRestriction extends Registry[Customizer.HintAnnounceRestriction]:
    val Disable : HintAnnounceRestriction = ( subscribableName : SubscribableName, subscriptionManager : SubscriptionManager, content : ItemContent ) => Some(Iffy.HintAnnounce.Policy.Always)

  private val AllRegistries = Set( Subject, Contents, MastoAnnouncement, TemplateParams, Filter )

  def warnAllUnknown( knownSubscribables : Set[SubscribableName] ) : Unit =
    AllRegistries.foreach( registry => registry.warnUnknown( knownSubscribables ) )

  abstract class Registry[T]:
    //MT: Protected by this' lock
    private val _registry = mutable.Map.empty[SubscribableName, T]

    @targetName("registerConveniently")
    def register( subscribableName : String, customizer : T ) : Unit = register( SubscribableName(subscribableName), customizer )

    def register( subscribableName : SubscribableName, customizer : T ) : Unit = this.synchronized:
      _registry.put( subscribableName, customizer )

    def retrieve( subscribableName : SubscribableName ) : Option[T] = this.synchronized:
      _registry.get( subscribableName )

    def warnUnknown( knownSubscribables : Set[SubscribableName] ) : Unit =
      val keys = this.synchronized( _registry.keySet.to(Set) )
      val warnables = keys -- knownSubscribables
      warnables.foreach: sn =>
        WARNING.log( s"${this}: A customizer has been registered for unknown subscribable '$sn'." )
