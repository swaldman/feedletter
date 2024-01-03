package com.mchange.feedletter

import untemplate.Untemplate

import scala.collection.mutable

import api.V0.SubscriptionStatusChanged

// We define this mutable(!) registry, rather than using IndexedUntemplates directly,
// because we may in future wish to define "binary" distributions that nevertheless
// permit the definition of new untemplates.
//
// Those distributions could index the untemplates under whatever name users like,
// but would need to add them to this registry at app startup.

object AllUntemplates:
  //MT: protected by this' lock
  private val _untemplates = mutable.Map.from( IndexedUntemplates )

  //MT: protected by this' lock
  private var _cache : LazyCache = new LazyCache

  class LazyCache:
    lazy val ComposeUntemplates         = AllUntemplates.all.filter( (k,v) => isCompose(v) )         map ( (k,v) => (k, v.asInstanceOf[Untemplate[ComposeInfo.Universal,Nothing]]) )
    lazy val ComposeUntemplatesSingle   = ComposeUntemplates.filter( (k,v) => isComposeSingle(v) )   map ( (k,v) => (k, v.asInstanceOf[Untemplate[ComposeInfo.Single,Nothing]])    )
    lazy val ComposeUntemplatesMultiple = ComposeUntemplates.filter( (k,v) => isComposeMultiple(v) ) map ( (k,v) => (k, v.asInstanceOf[Untemplate[ComposeInfo.Multiple,Nothing]])  )
    lazy val ConfirmUntemplates         = AllUntemplates.all.filter( (k,v) => isConfirm(v) )         map ( (k,v) => (k, v.asInstanceOf[Untemplate[ConfirmInfo,Nothing]]) )
    lazy val StatusChangedUntemplates   = AllUntemplates.all.filter( (k,v) => isStatusChanged(v) )   map ( (k,v) => (k, v.asInstanceOf[Untemplate[SubscriptionStatusChanged,Nothing]]) )

  def cache : LazyCache = this.synchronized( _cache )

  def compose         = cache.ComposeUntemplates
  def composeSingle   = cache.ComposeUntemplatesSingle
  def composeMultiple = cache.ComposeUntemplatesMultiple
  def confirm         = cache.ConfirmUntemplates
  def statusChanged   = cache.StatusChangedUntemplates

  def add( more : IterableOnce[(String,Untemplate.AnyUntemplate)] ) : Unit = this.synchronized:
    _untemplates.addAll(more)
    _cache = new LazyCache

  def all : Map[String,Untemplate.AnyUntemplate] = this.synchronized:
    Map.from( _untemplates )

  def findComposeUntemplateSingle( fqn : String ) : untemplate.Untemplate[ComposeInfo.Single,Nothing] =
    findComposeUntemplate(fqn, single=true).asInstanceOf[untemplate.Untemplate[ComposeInfo.Single,Nothing]]

  def findComposeUntemplateMultiple( fqn : String ) : untemplate.Untemplate[ComposeInfo.Multiple,Nothing] =
    findComposeUntemplate(fqn, single=false).asInstanceOf[untemplate.Untemplate[ComposeInfo.Multiple,Nothing]]

  def findConfirmUntemplate( fqn : String ) : untemplate.Untemplate[ConfirmInfo,Nothing] =
    this.confirm
      .get( fqn )
      .getOrElse:
        this.all.get( fqn ).fold( throw new UntemplateNotFound( s"Confirm untemplate '$fqn' does not appear to be defined." ) ): ut =>
          throw new UnsuitableUntemplate( s"'$fqn' appears not to be a confirm untemplate. (input type: ${untemplateInputType(ut)})" )

  def findStatusChangedUntemplate( fqn : String ) : untemplate.Untemplate[SubscriptionStatusChanged,Nothing] =
    this.statusChanged
      .get( fqn )
      .getOrElse:
        this.all.get( fqn ).fold( throw new UntemplateNotFound( s"Status-changed untemplate '$fqn' does not appear to be defined." ) ): ut =>
          throw new UnsuitableUntemplate( s"'$fqn' appears not to be a status-changed untemplate. (input type: ${untemplateInputType(ut)})" )

  private def findComposeUntemplate( fqn : String, single : Boolean ) : Untemplate.AnyUntemplate =
    val snapshot = this.cache
    val (expectedDesc, otherDesc, expectedLoc, otherLoc) =
      if single then
        ("single","multiple",snapshot.ComposeUntemplatesSingle,snapshot.ComposeUntemplatesMultiple)
      else
        ("multiple","single",snapshot.ComposeUntemplatesMultiple,snapshot.ComposeUntemplatesSingle)
    expectedLoc.get( fqn ).getOrElse:
      val isCrosswise = otherLoc.contains(fqn)
      if isCrosswise then
        throw new UnsuitableUntemplate( s"Compose untemplate '$fqn' is a ${otherDesc}-item-accepting untemplate, not available in contexts that render a ${expectedDesc} item." )
      else
        this.all.get( fqn ).fold( throw new UntemplateNotFound( s"Compose untemplate '$fqn' does not appear to be defined." ) ): ut =>
          throw new UnsuitableUntemplate( s"'$fqn' appears not to be a compose untemplate. (input type: ${untemplateInputType(ut)})" )

  private def isCompose( candidate : Untemplate.AnyUntemplate ) : Boolean =
    candidate.UntemplateInputTypeCanonical match
      case Some( ctype ) => ctype.startsWith("com.mchange.feedletter.ComposeInfo")
      case None =>
        val checkMe  = candidate.UntemplateInputTypeDeclared
        val prefixes = "ComposeInfo" :: "com.mchange.feedletter.ComposeInfo" :: "feedletter.ComposeInfo" :: Nil
        prefixes.find( checkMe.startsWith( _ ) ).nonEmpty

  private def isComposeSingle( candidate : Untemplate.AnyUntemplate ) : Boolean =
    candidate.UntemplateInputTypeCanonical match
      case Some( "com.mchange.feedletter.ComposeInfo.Single" ) => true
      case Some( _ ) => false
      case None =>
        val checkMe  = candidate.UntemplateInputTypeDeclared
        val prefixes = "ComposeInfo.Single" :: "com.mchange.feedletter.ComposeInfo.Single" :: "feedletter.ComposeInfo.Single" :: Nil
        prefixes.find( checkMe == _ ).nonEmpty

  private def isComposeMultiple( candidate : Untemplate.AnyUntemplate ) : Boolean =
    candidate.UntemplateInputTypeCanonical match
      case Some( "com.mchange.feedletter.ComposeInfo.Multiple" ) => true
      case Some( _ ) => false
      case None =>
        val checkMe  = candidate.UntemplateInputTypeDeclared
        val prefixes = "ComposeInfo.Multiple" :: "com.mchange.feedletter.ComposeInfo.Multiple" :: "feedletter.ComposeInfo.Multiple" :: Nil
        prefixes.find( checkMe == _ ).nonEmpty

  private def isConfirm( candidate : Untemplate.AnyUntemplate ) : Boolean =
    candidate.UntemplateInputTypeCanonical match
      case Some( ctype ) => ctype == "com.mchange.feedletter.ConfirmInfo"
      case None =>
        val checkMe  = candidate.UntemplateInputTypeDeclared
        val prefixes = "ConfirmInfo" :: "com.mchange.feedletter.ConfirmInfo" :: "feedletter.ConfirmInfo" :: Nil
        prefixes.find( checkMe.startsWith( _ ) ).nonEmpty

  private def isStatusChanged( candidate : Untemplate.AnyUntemplate ) : Boolean =
    candidate.UntemplateInputTypeCanonical match
      case Some( ctype ) => ctype == "com.mchange.feedletter.api.V0.SubscriptionStatusChanged"
      case None =>
        val checkMe  = candidate.UntemplateInputTypeDeclared
        val prefixes = "SubscriptionStatusChanged" :: "com.mchange.feedletter.api.V0.SubscriptionStatusChanged" :: "feedletter.api.V0.SubscriptionStatusChanged" :: Nil
        prefixes.find( checkMe.startsWith( _ ) ).nonEmpty


