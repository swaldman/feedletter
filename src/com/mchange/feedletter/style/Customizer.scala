package com.mchange.feedletter.style

import com.mchange.feedletter.*

import scala.collection.mutable

import scala.annotation.targetName

object Customizer:
  type Subject = ( subscribableName : SubscribableName, subscriptionManager : SubscriptionManager, withinTypeId : String, feedUrl : FeedUrl, contents : Set[ItemContent] ) => String
  type MastoAnnouncement = ( subscribableName : SubscribableName, subscriptionManager : SubscriptionManager, withinTypeId : String, feedUrl : FeedUrl, content : ItemContent ) => Option[String]
  type TemplateParams =
    ( subscribableName : SubscribableName, subscriptionManager : SubscriptionManager, withinTypeId : String, feedUrl : FeedUrl, destination : Destination, subscriptionId : SubscriptionId, removeLink : String ) => Map[String,String]

  object Subject           extends Registry[Customizer.Subject]
  object MastoAnnouncement extends Registry[Customizer.MastoAnnouncement]
  object TemplateParams    extends Registry[Customizer.TemplateParams]

  abstract class Registry[T]:
    //MT: Protected by this' lock
    private val _registry = mutable.Map.empty[SubscribableName, T]

    @targetName("registerConveniently")
    def register( subscribableName : String, customizer : T ) : Unit = register( SubscribableName(subscribableName), customizer )

    def register( subscribableName : SubscribableName, customizer : T ) : Unit = this.synchronized:
      _registry.put( subscribableName, customizer )

    def retrieve( subscribableName : SubscribableName ) : Option[T] = this.synchronized:
      _registry.get( subscribableName )
