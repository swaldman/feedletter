package com.mchange.feedletter

import java.sql.Connection
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoField

import scala.collection.immutable

import com.mchange.feedletter.db.{AssignableKey, AssignableWithinTypeInfo, ItemStatus}

import com.mchange.conveniences.www.*

object SubscriptionType:
  private val GeneralRegex = """^(\w+)\.(\w+)\:(.*)$""".r

  private def preparse( s : String ) : Option[Tuple3[String,String,Seq[Tuple2[String,String]]]] =
    s match
      case GeneralRegex( _type, _subtype, _fields ) => Some( Tuple3( _type, _subtype, wwwFormDecodeUTF8( _fields ) ) )
      case _ => None

  object Email:
    private def createBySubtype( subtype : String, tuples : Seq[Tuple2[String,String]] ) : Option[SubscriptionType.Email] =
      val tupMap = tuples.toMap
      tupMap.get("from").flatMap: from =>
        val replyTo = tupMap.get("replyTo")
        subtype match
          case Immediate.Subtype => Some( Immediate( from, replyTo ) )
          case Weekly.Subtype    => Some( Weekly( from, replyTo ) )
          case _                 => None

    def parse( s : String ) : Option[SubscriptionType.Email] =
      preparse(s) match
        case Some( Tuple3("Email", subtype, params) ) => createBySubtype( subtype, params )
        case _ => None

    object Immediate:
      val Subtype = "Immediate"
    case class Immediate( from : String, replyTo : Option[String] ) extends Email:
      override def subtype : String = Immediate.Subtype
      override def withinTypeId( feedUrl : String, lastCompleted : Option[AssignableWithinTypeInfo], mostRecentOpen : Option[AssignableWithinTypeInfo], guid : String, content : ItemContent, status : ItemStatus ) : Option[String] =
        Some( guid )
      override def isComplete( withinTypeId : String, currentCount : Int, now : Instant ) : Boolean = true
      /*
      def route( conn : Connection, feedUrl : String, withinTypeId : String, destination : String, contents : Set[ItemContent] ) : Unit =
        assert( contents.size == 1, s"EmailImmediate expects contents exactly one item from a completed assignable, found ${contents.size}. withinTypeId: ${withinTypeId}" )
        val contents = composeSingleItemHtmlMailContent( contents.head )
        ???
      */

    object Weekly:
      val Subtype = "Weekly"
    case class Weekly( from : String, replyTo : Option[String] ) extends Email:
      private val WtiFormatter = DateTimeFormatter.ofPattern("YYYY-'week'ww")
      override def subtype : String = Weekly.Subtype
      override def withinTypeId( feedUrl : String, lastCompleted : Option[AssignableWithinTypeInfo], mostRecentOpen : Option[AssignableWithinTypeInfo], guid : String, content : ItemContent, status : ItemStatus ) : Option[String] =
        Some( WtiFormatter.format( status.lastChecked ) ) 
      override def isComplete( withinTypeId : String, currentCount : Int, now : Instant ) : Boolean =
        val ta = WtiFormatter.parse( withinTypeId )
        val year = ta.get( ChronoField.YEAR )
        val woy = ta.get( ChronoField.ALIGNED_WEEK_OF_YEAR )
        val nowYear = now.get( ChronoField.YEAR )
        nowYear > year || (nowYear == year && now.get( ChronoField.ALIGNED_WEEK_OF_YEAR ) > woy)
  trait Email extends SubscriptionType:
    def subtype : String
    def from : String
    def replyTo : Option[String]
    def params : immutable.SortedSet[Tuple2[String,String]] = immutable.SortedSet( ("from",from) ) ++ replyTo.map( Tuple2("replyTo",_) )
    override def toString() : String = s"Email.${subtype}:${wwwFormEncodeUTF8( params.toSeq* )}"
  end Email

  def parse( str : String ) : SubscriptionType =
    val parsed =
      Email.parse( str ) // orElse ... whatever else we come up with
    parsed match
      case Some( stype ) => stype
      case other => throw new InvalidSubscriptionType(s"'${other}' could not be parsed into a valid subscription type.")
sealed trait SubscriptionType:
  def withinTypeId( feedUrl : String, lastCompleted : Option[AssignableWithinTypeInfo], mostRecentOpen : Option[AssignableWithinTypeInfo], guid : String, content : ItemContent, status : ItemStatus ) : Option[String]
  def isComplete( withinTypeId : String, currentCount : Int, now : Instant ) : Boolean
  def route( conn : Connection, assignableKey : AssignableKey, contents : Set[ItemContent], destinations : Set[String] ) : Unit = ??? // temporary



