package com.mchange.feedletter

import upickle.default._

import com.mchange.mailutil.Smtp

object Destination:
  val  Json = DestinationJson
  type Json = DestinationJson

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

  def materialize( json : Destination.Json ) : Destination = read[Destination]( json.toString )

  enum Tag:
    case Email, Sms, Mastodon

  private def toUJsonV1( destination : Destination ) : ujson.Value =
    def emf( email : Destination.Email )   : ujson.Obj = ujson.Obj.from(Seq("addressPart"->ujson.Str(email.addressPart)) ++ email.displayNamePart.map(dnp=>("displayNamePart"->ujson.Str(dnp))))
    def smsf( sms : Destination.Sms )      : ujson.Obj = ujson.Obj( "number" -> sms.number )
    def mf( masto : Destination.Mastodon ) : ujson.Obj = ujson.Obj( "name" -> masto.name, "instanceUrl" -> masto.instanceUrl )
    val (fields, tpe) =
      destination match
        case email : Destination.Email    => (emf(email), Tag.Email)
        case sms   : Destination.Sms      => (smsf(sms),  Tag.Sms)
        case masto : Destination.Mastodon => (mf(masto),  Tag.Mastodon)
    val headerFields =
       ujson.Obj(
         "version" -> 1,
         "type" -> tpe.toString
       )
    ujson.Obj.from(headerFields.obj ++ fields.obj)

  private def fromUJsonV1( jsonValue : ujson.Value ) : Destination =
    val obj = jsonValue.obj
    val version = obj.get("version").map( _.num.toInt )
    if version.nonEmpty && version != Some(1) then
      throw new InvalidDestination(s"Unpickling Destination, found version ${version.get}, expected version 1: " + obj.mkString(", "))
    else
      val tpe = obj("type").str
      Tag.valueOf(tpe) match
        case Tag.Email    => Destination.Email( addressPart = obj("addressPart").str, displayNamePart = obj.get("displayNamePart").map(_.str)) 
        case Tag.Sms      => Destination.Sms( number = obj("number").str )
        case Tag.Mastodon => Destination.Mastodon( name = obj("name").str, instanceUrl = obj("instanceUrl").str )

  given ReadWriter[Destination] =
    readwriter[ujson.Value].bimap[Destination](
      destination => toUJsonV1( destination ),
      jsonValue   => fromUJsonV1( jsonValue )
    )

sealed trait Destination extends Jsonable:
   /**
    *  Within any subscribable (subscription definiton), a String that should be
    *  kept unique, in order to avoid the possibility of people becoming annoyingly
    *  multiply subscribed.
    */
  def unique : String
  def json       : Destination.Json = Destination.Json( write[Destination]( this ) )
  def jsonPretty : Destination.Json = Destination.Json( write[Destination]( this, indent = 4 ) )

