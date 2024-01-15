package com.mchange.feedletter.style

import com.mchange.feedletter.*

import untemplate.Untemplate

import scala.collection.mutable
import scala.annotation.tailrec

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
    findXxxUntemplate( this.confirm, fqn, "confirm")

  def findStatusChangeUntemplate( fqn : String ) : untemplate.Untemplate[StatusChangeInfo,Nothing] =
    findXxxUntemplate( this.statusChange, fqn, "status-change")

  def findRemovalNotificationUntemplate( fqn : String ) : untemplate.Untemplate[RemovalNotificationInfo,Nothing] =
    findXxxUntemplate( this.removalNotification, fqn, "removal notification")

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

  private def findXxxUntemplate[T]( uts : Map[String,Untemplate[T,Nothing]], fqn : String, utypeLowerCased : String ) : Untemplate[T,Nothing] =
    uts.get( fqn ).getOrElse:
      this.all.get( fqn ).fold( throw new UntemplateNotFound( s"${utypeLowerCased.capitalize} untemplate '$fqn' does not appear to be defined." ) ): ut =>
        throw new UnsuitableUntemplate( s"'$fqn' appears not to be a ${utypeLowerCased} untemplate. (input type: ${untemplateInputType(ut)})" )

  private def canComposeSingle( candidate : Untemplate.AnyUntemplate ) : Boolean = isComposeSingle( candidate ) || isComposeUniversal( candidate )

  private def canComposeMultiple( candidate : Untemplate.AnyUntemplate ) : Boolean = isComposeMultiple( candidate ) || isComposeUniversal( candidate )

  private def suffixes(fqn : String) : Set[String] =
    val elements = fqn.split('.').toList

    @tailrec
    def build( from : List[String], accum : Set[List[String]] ) : Set[List[String]] =
      from match
        case head :: tail => build( tail, accum + from )
        case Nil          => accum
    build( elements, Set.empty ).map( _.mkString(".") )
  end suffixes

  private def isByInputTypeFqn( candidate : Untemplate.AnyUntemplate, fqn : String, suffixes : Set[String] ) : Boolean =
    candidate.UntemplateInputTypeCanonical match
      case Some( `fqn` ) => true
      case Some( _ )     => false
      case None          => suffixes.contains( candidate.UntemplateInputTypeDeclared )

  private def visibleType( clz : Class[?] ) = clz.getName.replace('$','.')

  private val FqnComposeInfoSingle       = visibleType(classOf[ComposeInfo.Single])
  private val FqnComposeInfoMultiple     = visibleType(classOf[ComposeInfo.Multiple])
  private val FqnComposeInfoUniversal    = visibleType(classOf[ComposeInfo.Universal])
  private val FqnConfirmInfo             = visibleType(classOf[ConfirmInfo])
  private val FqnStatusChangeInfo        = visibleType(classOf[StatusChangeInfo])
  private val FqnRemovalNotificationInfo = visibleType(classOf[RemovalNotificationInfo])
  
  private val SuffixesComposeInfoSingle       = suffixes( FqnComposeInfoSingle )
  private val SuffixesComposeInfoMultiple     = suffixes( FqnComposeInfoMultiple )
  private val SuffixesComposeInfoUniversal    = suffixes( FqnComposeInfoUniversal )
  private val SuffixesConfirmInfo             = suffixes( FqnConfirmInfo )
  private val SuffixesStatusChangeInfo        = suffixes( FqnStatusChangeInfo )
  private val SuffixesRemovalNotificationInfo = suffixes( FqnRemovalNotificationInfo )

  private def isComposeSingle( candidate : Untemplate.AnyUntemplate )       : Boolean = isByInputTypeFqn( candidate, FqnComposeInfoSingle, SuffixesComposeInfoSingle )
  private def isComposeMultiple( candidate : Untemplate.AnyUntemplate )     : Boolean = isByInputTypeFqn( candidate, FqnComposeInfoMultiple, SuffixesComposeInfoMultiple )
  private def isComposeUniversal( candidate : Untemplate.AnyUntemplate )    : Boolean = isByInputTypeFqn( candidate, FqnComposeInfoUniversal, SuffixesComposeInfoUniversal )
  private def isConfirm( candidate : Untemplate.AnyUntemplate )             : Boolean = isByInputTypeFqn( candidate, FqnConfirmInfo, SuffixesConfirmInfo )
  private def isStatusChange( candidate : Untemplate.AnyUntemplate )        : Boolean = isByInputTypeFqn( candidate, FqnStatusChangeInfo, SuffixesStatusChangeInfo )
  private def isRemovalNotification( candidate : Untemplate.AnyUntemplate ) : Boolean = isByInputTypeFqn( candidate, FqnRemovalNotificationInfo, SuffixesRemovalNotificationInfo )

  private def isCompose( candidate : Untemplate.AnyUntemplate ) = isComposeSingle( candidate ) || isComposeMultiple( candidate ) || isComposeUniversal( candidate )

