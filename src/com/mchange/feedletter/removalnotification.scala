package com.mchange.feedletter

case class RemovalNotificationInfo(
  subscriptionName     : String,
  subscriptionManager  : SubscriptionManager,
  destination          : Destination,
  resubscribeLink      : String
)
