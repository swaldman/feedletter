package com.mchange.feedletter

import untemplate.Untemplate

case class ConfirmInfo( destination : Destination, subscribableName : SubscribableName, subscriptionManager : SubscriptionManager, confirmGetLink : String, confirmHours : Int ):
  def subscriptionName = subscribableName.toString()

