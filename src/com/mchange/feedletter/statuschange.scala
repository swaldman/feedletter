package com.mchange.feedletter

case class StatusChangeInfo( change : SubscriptionStatusChange, subscribableName : SubscribableName, destination : Destination, resubscribeLink : String )
