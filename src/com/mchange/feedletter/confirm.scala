package com.mchange.feedletter

import untemplate.Untemplate

case class ConfirmInfo( destination : Destination, subscribableName : SubscribableName, subscriptionManager : SubscriptionManager, subscriptionId : SubscriptionId, secretSalt : String )

