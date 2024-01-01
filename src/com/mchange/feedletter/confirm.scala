package com.mchange.feedletter

import untemplate.Untemplate

case class ConfirmInfo( destination : Destination, subscribableName : SubscribableName, subscriptionManager : SubscriptionManager, confirmGetLink : String ):
  def subscriptionName = subscribableName.toString()

