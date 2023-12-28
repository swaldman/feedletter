package com.mchange.feedletter

import untemplate.Untemplate

case class ConfirmInfo( destination : Destination, subscribableName : SubscribableName, subscriptionManager : SubscriptionManager, subscriptionId : SubscriptionId, secretSalt : String )

val ConfirmUntemplates = AllUntemplates.get.filter( (k,v) => isConfirm(v) ).map( (k,v) => (k, v.asInstanceOf[Untemplate[ConfirmInfo,Nothing]]) )

def isConfirm( candidate : Untemplate.AnyUntemplate ) : Boolean =
  candidate.UntemplateInputTypeCanonical match
    case Some( ctype ) => ctype == "com.mchange.feedletter.ConfirmInfo"
    case None =>
      val checkMe  = candidate.UntemplateInputTypeDeclared
      val prefixes = "ConfirmInfo" :: "com.mchange.feedletter.ConfirmInfo" :: "feedletter.ConfirmInfo" :: Nil
      prefixes.find( checkMe.startsWith( _ ) ).nonEmpty

def findConfirmUntemplate( fqn : String ) : untemplate.Untemplate[ConfirmInfo,Nothing] =
  ConfirmUntemplates
    .get( fqn )
    .map( _.asInstanceOf[untemplate.Untemplate[ConfirmInfo,Nothing]] )
    .getOrElse:
      throw new UntemplateNotFound( s"Confirm untemplate '$fqn' does not appear to be defined." )

