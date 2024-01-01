package com.mchange.feedletter

import com.github.plokhotnyuk.jsoniter_scala.macros.*
import com.github.plokhotnyuk.jsoniter_scala.core.*

import com.mchange.mailutil.Smtp

object Destination:
  val  Json = DestinationJson
  type Json = DestinationJson

  given JsonValueCodec[Destination] = JsonCodecMaker.make

  object Email:
    def apply( address : Smtp.Address ) : Email = Email( address.email, address.displayName )
  case class Email( addressPart : String, displayNamePart : Option[String] ) extends Destination:
    lazy val toAddress : Smtp.Address = Smtp.Address( addressPart, displayNamePart )
    lazy val rendered = toAddress.rendered
    override def unique = s"e-mail:${addressPart}"

  case class Mastodon( name : String, instanceUrl : String ) extends Destination:
    override def unique = s"mastodon:${instanceUrl}"

  case class Sms( number : String ) extends Destination:
    override def unique = s"sms:${number}"

  def materialize( json : Destination.Json ) : Destination = readFromString[Destination]( json.toString() )

sealed trait Destination extends Jsonable:
   /**
    *  Within any subscribable (subscription definiton), a String that should be
    *  kept unique, in order to avoid the possibility of people becoming annoyingly
    *  multiply subscribed.
    */
  def unique : String
  def json       : Destination.Json = Destination.Json( writeToString[Destination]( this ) )
  def jsonPretty : Destination.Json = Destination.Json( writeToString[Destination](this, WriterConfig.withIndentionStep(4)) )

