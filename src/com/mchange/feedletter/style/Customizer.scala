package com.mchange.feedletter.style

import com.mchange.feedletter.*

import scala.collection.mutable

import scala.annotation.targetName

import MLevel.*

object Customizer extends SelfLogging:
  type Subject = ( subscribableName : SubscribableName, subscriptionManager : SubscriptionManager, withinTypeId : String, feedUrl : FeedUrl, contents : Seq[ItemContent] ) => String
  type Contents = ( subscribableName : SubscribableName, subscriptionManager : SubscriptionManager, withinTypeId : String, feedUrl : FeedUrl, contents : Seq[ItemContent] ) => Seq[ItemContent]
  type MastoAnnouncement = ( subscribableName : SubscribableName, subscriptionManager : SubscriptionManager, withinTypeId : String, feedUrl : FeedUrl, content : ItemContent ) => Option[String]
  type TemplateParams =
    ( subscribableName : SubscribableName, subscriptionManager : SubscriptionManager, withinTypeId : String, feedUrl : FeedUrl, destination : Destination, subscriptionId : SubscriptionId, removeLink : String ) => Map[String,String]

  object Subject           extends Registry[Customizer.Subject]
  object Contents          extends Registry[Customizer.Contents]
  object MastoAnnouncement extends Registry[Customizer.MastoAnnouncement]
  object TemplateParams    extends Registry[Customizer.TemplateParams]

  private val AllRegistries = Set( Subject, Contents, MastoAnnouncement, TemplateParams )

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
