package com.mchange.feedletter

import java.sql.Connection
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoField

import scala.collection.immutable

import com.mchange.feedletter.db.{AssignableKey, AssignableWithinTypeInfo, ItemStatus}

import com.mchange.conveniences.www.*
import com.mchange.feedletter.db.PgDatabase

object SubscriptionType:
  private val GeneralRegex = """^(\w+)\.(\w+)\:(.*)$""".r

  object Email:
    class Immediate( params : Seq[(String,String)] ) extends Email("Immediate", params):
      override def withinTypeId( feedUrl : String, lastCompleted : Option[AssignableWithinTypeInfo], mostRecentOpen : Option[AssignableWithinTypeInfo], guid : String, content : ItemContent, status : ItemStatus ) : Option[String] =
        Some( guid )
      override def isComplete( withinTypeId : String, currentCount : Int, now : Instant ) : Boolean = true
      override def route( conn : Connection, assignableKey : AssignableKey, contents : Set[ItemContent], destinations : Set[String] ) : Unit =
        assert( contents.size == 1, s"Email.Immediate expects contents exactly one item from a completed assignable, found ${contents.size}. assignableKey: ${assignableKey}" )
        val fullContents = composeSingleItemHtmlMailContent( assignableKey, this, contents.head )
        PgDatabase.queueContentsForMailing( conn, fullContents, destinations )

    class Weekly( params : Seq[(String,String)] ) extends Email( "Weekly", params ):
      private val WtiFormatter = DateTimeFormatter.ofPattern("YYYY-'week'ww")
      override def withinTypeId( feedUrl : String, lastCompleted : Option[AssignableWithinTypeInfo], mostRecentOpen : Option[AssignableWithinTypeInfo], guid : String, content : ItemContent, status : ItemStatus ) : Option[String] =
        Some( WtiFormatter.format( status.lastChecked ) ) 
      override def isComplete( withinTypeId : String, currentCount : Int, now : Instant ) : Boolean =
        val ta = WtiFormatter.parse( withinTypeId )
        val year = ta.get( ChronoField.YEAR )
        val woy = ta.get( ChronoField.ALIGNED_WEEK_OF_YEAR )
        val nowYear = now.get( ChronoField.YEAR )
        nowYear > year || (nowYear == year && now.get( ChronoField.ALIGNED_WEEK_OF_YEAR ) > woy)
      override def route( conn : Connection, assignableKey : AssignableKey, contents : Set[ItemContent], destinations : Set[String] ) : Unit =
        val fullContents = composeMultipleItemHtmlMailContent( assignableKey, this, contents )
        PgDatabase.queueContentsForMailing( conn, fullContents, destinations )

  abstract class Email(subtype : String, params : Seq[Tuple2[String,String]]) extends SubscriptionType("Email", subtype, params):
    val from    : Seq[String] = params.filter( _(0) == "from" ).map( _(1) )
    val replyTo : Seq[String] = params.filter( _(0) == "replyTo" ).map( _(1) )

    if from.isEmpty then
      throw new InvalidSubscriptionType(
        "An Email subscription type must include at least one 'from' paramm. Params given: " +
        params.map( (k,v) => k + " -> " + v ).mkString(", ")
      )
  end Email

  def dispatch( category : String, subtype : String, params : Seq[(String,String)] ) : Option[SubscriptionType] =
    ( category, subtype ) match
      case ("Email", "Immediate") => Some( Email.Immediate(params) )
      case ("Email", "Weekly")    => Some( Email.Weekly(params) )
      case _                      => None 

  private def preparse( s : String ) : Option[Tuple3[String,String,Seq[Tuple2[String,String]]]] =
    s match
      case GeneralRegex( category, subtype, params ) => Some( Tuple3( category, subtype, wwwFormDecodeUTF8( params ) ) )
      case _ => None

  def parse( str : String ) : SubscriptionType =
      preparse( str ).flatMap( dispatch.tupled ).getOrElse:
        throw new InvalidSubscriptionType(s"'${str}' could not be parsed into a valid subscription type.")

sealed abstract class SubscriptionType( val category : String, val subtype : String, val params : Seq[(String,String)] ):
  def withinTypeId( feedUrl : String, lastCompleted : Option[AssignableWithinTypeInfo], mostRecentOpen : Option[AssignableWithinTypeInfo], guid : String, content : ItemContent, status : ItemStatus ) : Option[String]
  def isComplete( withinTypeId : String, currentCount : Int, now : Instant ) : Boolean
  def route( conn : Connection, assignableKey : AssignableKey, contents : Set[ItemContent], destinations : Set[String] ) : Unit = ??? // XXX: temporary, make abstract when we stabilize
  override def toString() : String = s"${category}.${subtype}:${wwwFormEncodeUTF8( params.toSeq* )}"
  override def equals( other : Any ) : Boolean =
    other match
      case stype : SubscriptionType =>
        category == category && subtype == subtype && params == params
      case _ =>
        false
  override def hashCode(): Int =
    category.## ^ subtype.## ^ params.##





