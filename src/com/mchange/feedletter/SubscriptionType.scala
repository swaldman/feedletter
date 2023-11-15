package com.mchange.feedletter

import java.time.format.DateTimeFormatter

import com.mchange.feedletter.db.ItemStatus

object SubscriptionType:
  case object Immediate extends SubscriptionType:
    override def toString(): String = "Immediate"
    override def withinTypeId( feedUrl : String, guid : String, content : ItemContent, status : ItemStatus ) : Option[String] = Some( guid )
  case object Weekly extends SubscriptionType:
    private val Formatter = DateTimeFormatter.ofPattern("YYYY-'week'ww")
    override def toString(): String = "Weekly"
    override def withinTypeId( feedUrl : String, guid : String, content : ItemContent, status : ItemStatus ) : Option[String] = Some( Formatter.format( status.lastChecked ) ) 
  def parse( str : String ) =
    str match
      case "Immediate" => Immediate
      case "Weekly"    => Weekly
      case other       => throw new InvalidSubscriptionType(s"'${other}' could not be parsed into a valid subscription type.")
trait SubscriptionType:
  def withinTypeId( feedUrl : String, guid : String, content : ItemContent, status : ItemStatus ) : Option[String]



