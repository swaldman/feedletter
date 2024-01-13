package com.mchange.feedletter

import untemplate.Untemplate

import scala.collection.mutable

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
    lazy val ComposeUntemplates             = AllUntemplates.all.filter( (k,v) => isCompose(v) )             map ( (k,v) => (k, v.asInstanceOf[Untemplate[ComposeInfo.Universal,Nothing]]) )
    lazy val ComposeUntemplatesSingle       = ComposeUntemplates.filter( (k,v) => canComposeSingle(v) )      map ( (k,v) => (k, v.asInstanceOf[Untemplate[ComposeInfo.Single,Nothing]])    )
    lazy val ComposeUntemplatesMultiple     = ComposeUntemplates.filter( (k,v) => canComposeMultiple(v) )    map ( (k,v) => (k, v.asInstanceOf[Untemplate[ComposeInfo.Multiple,Nothing]])  )
    lazy val ConfirmUntemplates             = AllUntemplates.all.filter( (k,v) => isConfirm(v) )             map ( (k,v) => (k, v.asInstanceOf[Untemplate[ConfirmInfo,Nothing]]) )
    lazy val StatusChangeUntemplates        = AllUntemplates.all.filter( (k,v) => isStatusChange(v) )        map ( (k,v) => (k, v.asInstanceOf[Untemplate[StatusChangeInfo,Nothing]]) )
    lazy val RemovalNotificationUntemplates = AllUntemplates.all.filter( (k,v) => isRemovalNotification(v) ) map ( (k,v) => (k, v.asInstanceOf[Untemplate[RemovalNotificationInfo,Nothing]]) )

  def cache : LazyCache = this.synchronized( _cache )

  def compose             = cache.ComposeUntemplates
  def composeSingle       = cache.ComposeUntemplatesSingle
  def composeMultiple     = cache.ComposeUntemplatesMultiple
  def confirm             = cache.ConfirmUntemplates
  def statusChange        = cache.StatusChangeUntemplates
  def removalNotification = cache.RemovalNotificationUntemplates

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

  def findStatusChangeUntemplate( fqn : String ) : untemplate.Untemplate[StatusChangeInfo,Nothing] =
    this.statusChange
      .get( fqn )
      .getOrElse:
        this.all.get( fqn ).fold( throw new UntemplateNotFound( s"Status-change untemplate '$fqn' does not appear to be defined." ) ): ut =>
          throw new UnsuitableUntemplate( s"'$fqn' appears not to be a status-change untemplate. (input type: ${untemplateInputType(ut)})" )

  def findRemovalNotificationUntemplate( fqn : String ) : untemplate.Untemplate[RemovalNotificationInfo,Nothing] =
    this.removalNotification
      .get( fqn )
      .getOrElse:
        this.all.get( fqn ).fold( throw new UntemplateNotFound( s"Removal notification untemplate '$fqn' does not appear to be defined." ) ): ut =>
          throw new UnsuitableUntemplate( s"'$fqn' appears not to be a removal notification. (input type: ${untemplateInputType(ut)})" )

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

  private def canComposeSingle( candidate : Untemplate.AnyUntemplate ) : Boolean = isComposeSingle( candidate ) || isComposeUniversal( candidate )
  
  private def isComposeSingle( candidate : Untemplate.AnyUntemplate ) : Boolean =
    candidate.UntemplateInputTypeCanonical match
      case Some( "com.mchange.feedletter.ComposeInfo.Single" ) => true
      case Some( _ ) => false
      case None =>
        val checkMe  = candidate.UntemplateInputTypeDeclared
        val prefixes = "ComposeInfo.Single" :: "com.mchange.feedletter.ComposeInfo.Single" :: "feedletter.ComposeInfo.Single" :: Nil
        prefixes.find( checkMe == _ ).nonEmpty

  private def canComposeMultiple( candidate : Untemplate.AnyUntemplate ) : Boolean = isComposeMultiple( candidate ) || isComposeUniversal( candidate )

  private def isComposeMultiple( candidate : Untemplate.AnyUntemplate ) : Boolean =
    candidate.UntemplateInputTypeCanonical match
      case Some( "com.mchange.feedletter.ComposeInfo.Multiple" ) => true
      case Some( _ ) => false
      case None =>
        val checkMe  = candidate.UntemplateInputTypeDeclared
        val prefixes = "ComposeInfo.Multiple" :: "com.mchange.feedletter.ComposeInfo.Multiple" :: "feedletter.ComposeInfo.Multiple" :: Nil
        prefixes.find( checkMe == _ ).nonEmpty

  private def isComposeUniversal( candidate : Untemplate.AnyUntemplate ) : Boolean =
    candidate.UntemplateInputTypeCanonical match
      case Some( "com.mchange.feedletter.ComposeInfo.Universal" ) => true
      case Some( _ ) => false
      case None =>
        val checkMe  = candidate.UntemplateInputTypeDeclared
        val prefixes = "ComposeInfo.Universal" :: "com.mchange.feedletter.ComposeInfo.Universal" :: "feedletter.ComposeInfo.Universal" :: Nil
        prefixes.find( checkMe == _ ).nonEmpty
        
  private def isConfirm( candidate : Untemplate.AnyUntemplate ) : Boolean =
    candidate.UntemplateInputTypeCanonical match
      case Some( ctype ) => ctype == "com.mchange.feedletter.ConfirmInfo"
      case None =>
        val checkMe  = candidate.UntemplateInputTypeDeclared
        val prefixes = "ConfirmInfo" :: "com.mchange.feedletter.ConfirmInfo" :: "feedletter.ConfirmInfo" :: Nil
        prefixes.find( checkMe.startsWith( _ ) ).nonEmpty

  private def isStatusChange( candidate : Untemplate.AnyUntemplate ) : Boolean =
    candidate.UntemplateInputTypeCanonical match
      case Some( ctype ) => ctype == "com.mchange.feedletter.StatusChangeInfo"
      case None =>
        val checkMe  = candidate.UntemplateInputTypeDeclared
        val prefixes = "StatusChangeInfo" :: "com.mchange.feedletter.StatusChangeInfo" :: "feedletter.StatusChangeInfo" :: Nil
        prefixes.find( checkMe.startsWith( _ ) ).nonEmpty

  private def isRemovalNotification( candidate : Untemplate.AnyUntemplate ) : Boolean =
    candidate.UntemplateInputTypeCanonical match
      case Some( ctype ) => ctype == "com.mchange.feedletter.RemovalNotificationInfo"
      case None =>
        val checkMe  = candidate.UntemplateInputTypeDeclared
        val prefixes = "RemovalNotificationInfo" :: "com.mchange.feedletter.RemovalNotificationInfo" :: "feedletter.RemovalNotificationInfo" :: Nil
        prefixes.find( checkMe.startsWith( _ ) ).nonEmpty


