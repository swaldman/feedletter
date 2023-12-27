package com.mchange.feedletter

import com.github.plokhotnyuk.jsoniter_scala.macros.*
import com.github.plokhotnyuk.jsoniter_scala.core.*

import com.mchange.mailutil.Smtp

object Destination:
  val  Json = DestinationJson
  type Json = DestinationJson

  trait Factory[+T <: Destination]:
    def fromJson( json : Json ) : T
    def tag : Tag

  object Tag:
    def apply( s : String ) : Tag = s
  opaque type Tag = String

  val Factories : Map[Tag,Factory[Destination]] =
    val allFactories : Seq[Factory[Destination]] = Email :: Mastodon :: Sms :: Nil
    allFactories.map( f => ( f.tag, f ) ).toMap

  object Email extends Factory[Email]:
    given JsonValueCodec[Email] = JsonCodecMaker.make
    def apply( address : Smtp.Address ) : Email = Email( address.email, address.displayName )
    def fromJson( json : Json ) : Email = readFromString[Email]( json.toString() )
    def tag : Tag = "Email"
  case class Email( addressPart : String, displayNamePart : Option[String] ) extends Destination:
    lazy val toAddress : Smtp.Address = Smtp.Address( addressPart, displayNamePart )
    lazy val rendered = toAddress.rendered
    override val factory = Email
    override def unique = s"e-mail:${addressPart}"
    override lazy val json = Json( writeToString(this) )
    override lazy val jsonPretty = Json( writeToString(this, WriterConfig.withIndentionStep(4)) )

  object Mastodon extends Factory[Mastodon]:
    given JsonValueCodec[Mastodon] = JsonCodecMaker.make
    def fromJson( json : Json ) : Mastodon = readFromString[Mastodon]( json.toString() )
    def tag : Tag = "Mastodon"
  case class Mastodon( name : String, instanceUrl : String ) extends Destination:
    override val factory = Mastodon
    override def unique = s"mastodon:${instanceUrl}"
    override lazy val json = Json( writeToString(this) )
    override lazy val jsonPretty = Json( writeToString(this, WriterConfig.withIndentionStep(4)) )

  object Sms extends Factory[Sms]:
    given JsonValueCodec[Sms] = JsonCodecMaker.make
    def fromJson( json : Json ) : Sms = readFromString[Sms]( json.toString() )
    def tag : Tag = "Sms"
  case class Sms( number : String ) extends Destination:
    override val factory = Sms
    override def unique = s"sms:${number}"
    override lazy val json = Json( writeToString(this) )
    override lazy val jsonPretty = Json( writeToString(this, WriterConfig.withIndentionStep(4)) )

  def materialize( tag : Tag, json : Json ) : Destination =
    val factory = Factories.get(tag).getOrElse:
      throw new InvalidDestination(s"Tag '${tag}' unknown for $json")
    factory.fromJson( json )

sealed trait Destination extends Jsonable:
   /**
    *  Within any subscribable (subscription definiton), a String that should be
    *  kept unique, in order to avoid the possibility of people becoming annoyingly
    *  multiply subscribed.
    */
  def unique : String
  def factory : Destination.Factory[Destination]
  def tag : Destination.Tag = factory.tag
  def json : Destination.Json
  def jsonPretty : Destination.Json

