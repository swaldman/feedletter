package com.mchange.feedletter

import java.sql.Connection
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoField

import com.mchange.feedletter.db.ItemStatus
import com.mchange.feedletter.db.AssignableWithinTypeInfo

object SubscriptionType:
  case object EmailImmediate extends SubscriptionType:
    override def toString(): String = "Immediate"
    override def withinTypeId( feedUrl : String, lastCompleted : Option[AssignableWithinTypeInfo], mostRecentOpen : Option[AssignableWithinTypeInfo], guid : String, content : ItemContent, status : ItemStatus ) : Option[String] =
      Some( guid )
    override def isComplete( withinTypeId : String, currentCount : Int, now : Instant ) : Boolean = true
/*
    def route( conn : Connection, feedUrl : String, withinTypeId : String, destination : String, contents : Set[ItemContent] ) : Unit =
      assert( contents.size == 1, s"EmailImmediate expects contents exactly one item from a completed assignable, found ${contents.size}. withinTypeId: ${withinTypeId}" )
      val contents = composeSingleItemHtmlMailContent( contents.head )
      ???
*/
  case object EmailWeekly extends SubscriptionType:
    private val Formatter = DateTimeFormatter.ofPattern("YYYY-'week'ww")
    override def toString(): String = "Weekly"
    override def withinTypeId( feedUrl : String, lastCompleted : Option[AssignableWithinTypeInfo], mostRecentOpen : Option[AssignableWithinTypeInfo], guid : String, content : ItemContent, status : ItemStatus ) : Option[String] =
      Some( Formatter.format( status.lastChecked ) ) 
    override def isComplete( withinTypeId : String, currentCount : Int, now : Instant ) : Boolean =
      val ta = Formatter.parse( withinTypeId )
      val year = ta.get( ChronoField.YEAR )
      val woy = ta.get( ChronoField.ALIGNED_WEEK_OF_YEAR )
      val nowYear = now.get( ChronoField.YEAR )
      nowYear > year || (nowYear == year && now.get( ChronoField.ALIGNED_WEEK_OF_YEAR ) > woy)
  def parse( str : String ) : SubscriptionType =
    str match
      case "EmailImmediate" => EmailImmediate
      case "EmailWeekly"    => EmailWeekly
      case other       => throw new InvalidSubscriptionType(s"'${other}' could not be parsed into a valid subscription type.")
trait SubscriptionType:
  def withinTypeId( feedUrl : String, lastCompleted : Option[AssignableWithinTypeInfo], mostRecentOpen : Option[AssignableWithinTypeInfo], guid : String, content : ItemContent, status : ItemStatus ) : Option[String]
  def isComplete( withinTypeId : String, currentCount : Int, now : Instant ) : Boolean
  def route( conn : Connection, feedUrl : String, withinTypeId : String, contents : Set[ItemContent], destinations : Set[String] ) : Unit = ??? // temporary



