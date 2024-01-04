package com.mchange.feedletter

case class StatusChangeInfo( change : SubscriptionStatusChange, subscriptionName : String, subscriptionManager : SubscriptionManager, destination : Destination, resubscribeLink : String )
