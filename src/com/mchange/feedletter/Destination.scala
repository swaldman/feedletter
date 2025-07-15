package com.mchange.feedletter

import upickle.default.*

import com.mchange.conveniences.www.*
import com.mchange.mailutil.Smtp
import scala.collection.StringOps

// Note: CSV headers and fields that need to be quoted
//       are quoted already when generated via `csvRowHeaders` and `toCsvRow`.
//       Just put commas between 'em and newlines at their end
object Destination:
  val  Json = DestinationJson
  type Json = DestinationJson

  trait TypeMetaInfo[T <: Destination]:
    def csvRowHeaders : Seq[String]

  private def q(s : String) = s""""$s""""

  object CsvRowHeaders:
    val Email    = Seq(q("E-Mail"), q("Display Name"))
    val Mastodon = Seq(q("Instance URL"), q("Name"))
    val Sms      = Seq(q("Phone Number"))
    val BlueSky  = Seq(q("Entryway URL"), q("Identifier"))
  given TypeMetaInfo[Email] = new TypeMetaInfo[Email] { def csvRowHeaders : Seq[String] = CsvRowHeaders.Email }
  given TypeMetaInfo[Mastodon] = new TypeMetaInfo[Mastodon] { def csvRowHeaders : Seq[String] = CsvRowHeaders.Mastodon }
  given TypeMetaInfo[Sms] = new TypeMetaInfo[Sms] { def csvRowHeaders : Seq[String] = CsvRowHeaders.Sms }
  given TypeMetaInfo[BlueSky] = new TypeMetaInfo[BlueSky] { def csvRowHeaders : Seq[String] = CsvRowHeaders.BlueSky }

  def csvRowHeaders[T <: Destination](using TypeMetaInfo[T]) =
    summon[TypeMetaInfo[T]].csvRowHeaders

  private object Tag:
    def valueOfIgnoreCaseOption( s : String ) : Option[Tag] = Tag.values.find( _.toString.equalsIgnoreCase(s) )
  private enum Tag:
    case Email, Sms, Mastodon, BlueSky

  private enum Key:
    def s = this.toString
    case addressPart,displayNamePart,address,displayName,number,name,instanceUrl,version,`type`,destinationType,entrywayUrl,identifier

  object Email:
    def apply( address : Smtp.Address ) : Email = Email( address.email, address.displayName )
    def apply( address : String )       : Email = this.apply( Smtp.Address.parseSingle( address ) )
  case class Email( addressPart : String, displayNamePart : Option[String] ) extends Destination:
    lazy val toAddress : Smtp.Address = Smtp.Address( addressPart, displayNamePart )
    lazy val rendered = toAddress.rendered
    override def unique = s"e-mail:${addressPart}"
    override def toFields = Seq( Key.destinationType.s -> Tag.Email.toString, Key.addressPart.s -> this.addressPart) ++ this.displayNamePart.map( dnp => Key.displayNamePart.s -> dnp )
    override def shortDesc : String = this.addressPart 
    override def fullDesc : String = this.rendered
    override def defaultDesc : String = fullDesc
    override def toCsvRow : Seq[String] = Seq( addressPart, q(displayNamePart.getOrElse("")) )
    override lazy val whyInvalid : Option[Throwable] = this.toAddress.whyInvalid
  case class Mastodon( name : String, instanceUrl : String ) extends Destination:
    override def unique = "mastodon:" + wwwFormEncodeUTF8((Key.name.s,name),(Key.instanceUrl.s,instanceUrl))
    override def toFields = Seq( Key.destinationType.s -> Tag.Mastodon.toString, Key.name.s -> this.name, Key.instanceUrl.s -> this.instanceUrl )
    override def shortDesc : String = this.instanceUrl
    override def fullDesc : String = s"Mastodon instance nicknamed '${name}' at ${instanceUrl}"
    override def defaultDesc : String = shortDesc
    override def toCsvRow : Seq[String] = Seq( instanceUrl, q(name) )
  case class Sms( number : String ) extends Destination:
    override def unique = s"sms:${number}"
    override def toFields = Seq( Key.destinationType.s -> Tag.Sms.toString, Key.number.s -> this.number )
    override def shortDesc : String = this.number
    override def fullDesc : String = s"SMS destination '${number}'"
    override def defaultDesc : String = shortDesc
    override def toCsvRow : Seq[String] = Seq( q(number) )
  case class BlueSky( entrywayUrl : String, identifier : String  ) extends Destination:
    override def unique = "bluesky:" + wwwFormEncodeUTF8((Key.entrywayUrl.s,entrywayUrl),(Key.identifier.s,identifier))
    override def toFields = Seq( Key.destinationType.s -> Tag.BlueSky.toString, Key.entrywayUrl.s -> this.entrywayUrl, Key.identifier.s -> this.identifier )
    override def shortDesc : String = s"${identifier} at ${entrywayUrl}"
    override def fullDesc : String = s"Bluesky account identified as '${identifier}' at entryway '${entrywayUrl}'"
    override def defaultDesc : String = shortDesc
    override def toCsvRow : Seq[String] = Seq( entrywayUrl, identifier )

  def materialize( json : Destination.Json ) : Destination = read[Destination]( json.toString )

  object fromFields:
    private class CarefulMap( val rawFields : Seq[(String,String)] ):
      val fields = rawFields.filter( (k,v) => k.trim.nonEmpty && v.trim.nonEmpty ) // neither blank keys or values are acceptabl
      val dupKeys = fields.toSet.groupBy( _(0) ).filter( _(1).size > 1 ).keySet
      val asMap = fields.toMap
      def get( k : String ) : Option[String] =
        if dupKeys(k) then
          val values = fields.collect { case (`k`, v) => v }
          throw new DuplicateKey( s"Cannot resolve unique value for field '$k'. Values: " + values.mkString(", ") )
        else
          asMap.get(k)
    def email( fields : Seq[(String,String)] ) : Option[Destination.Email] = email( CarefulMap(fields) )
    private def email( fmap : CarefulMap ) : Option[Destination.Email] =
      def fromFieldPair( kap : String, kdnp : String ) : Option[Destination.Email] =
        for
          ap <- fmap.get(kap)
        yield
          fmap.get(kdnp).fold( Email(Smtp.Address.parseSingle(ap)) )( dnp => Email( addressPart=ap, displayNamePart=Some(dnp) ) )
      def fromActualFields = fromFieldPair(Key.addressPart.s,Key.displayNamePart.s)
      def fromFriendlierFields = fromFieldPair(Key.address.s,Key.displayName.s)
      fromActualFields orElse fromFriendlierFields
    def mastodon( fields : Seq[(String,String)] ) : Option[Destination.Mastodon] = mastodon( CarefulMap( fields ) )
    private def mastodon( fmap : CarefulMap ) : Option[Destination.Mastodon] =
      for
        nm <- fmap.get( Key.name.s )
        iu <- fmap.get( Key.instanceUrl.s )
      yield
        Mastodon( name = nm, instanceUrl = iu )
    def sms( fields : Seq[(String,String)] ) : Option[Destination.Sms] = sms( CarefulMap( fields ) )
    private def sms( fmap : CarefulMap ) : Option[Destination.Sms] =
      for
        nu <- fmap.get( Key.number.s )
      yield
        Sms( number = nu )
    def bluesky( fields : Seq[(String,String)] ) : Option[Destination.BlueSky] = bluesky( CarefulMap( fields ) )
    private def bluesky( fmap : CarefulMap ) : Option[Destination.BlueSky] =
      for
        eu <- fmap.get( Key.entrywayUrl.s )
        id <- fmap.get( Key.identifier.s )
      yield
        BlueSky( entrywayUrl = eu, identifier = id )
    private def byType( tpe : String, fmap : CarefulMap ) : Option[Destination] =
      Tag.valueOfIgnoreCaseOption(tpe) match
        case Some( Tag.Email )    => email(fmap)
        case Some( Tag.Mastodon ) => mastodon(fmap)
        case Some( Tag.Sms )      => sms(fmap)
        case Some( Tag.BlueSky )  => bluesky(fmap)
        case None                 => None
    def apply( fields : Seq[(String,String)] ) : Option[Destination] = apply( CarefulMap(fields) )
    private def apply( fmap : CarefulMap ) : Option[Destination] =
      val tpe = fmap.get(Key.destinationType.s) orElse fmap.get(Key.`type`.s)
      tpe match
        case Some( t ) => byType(t,fmap)
        case None =>
          val destinations = Set( email(fmap), mastodon(fmap), sms(fmap) ).collect { case Some(dest) => dest }
          destinations.size match
            case 0 => None
            case 1 => Some(destinations.head)
            case n => throw new AmbiguousDestination( s"""Fields '${fmap.fields.mkString(", ")}' can be interpreted as multiple ($n) Destinations: ${destinations.mkString(", ")}""" )

  private def toUJsonV1( destination : Destination ) : ujson.Value =
    def emf( email : Destination.Email )   : ujson.Obj = ujson.Obj.from(Seq(Key.addressPart.s->ujson.Str(email.addressPart)) ++ email.displayNamePart.map(dnp=>(Key.displayNamePart.s->ujson.Str(dnp))))
    def smsf( sms : Destination.Sms )      : ujson.Obj = ujson.Obj( Key.number.s -> sms.number )
    def mf( masto : Destination.Mastodon ) : ujson.Obj = ujson.Obj( Key.name.s -> masto.name, Key.instanceUrl.s -> masto.instanceUrl )
    def bsf( bsky : Destination.BlueSky )  : ujson.Obj = ujson.Obj( Key.entrywayUrl.s -> bsky.entrywayUrl, Key.identifier.s -> bsky.identifier )
    val (fields, tpe) =
      destination match
        case email : Destination.Email    => (emf(email), Tag.Email)
        case sms   : Destination.Sms      => (smsf(sms),  Tag.Sms)
        case masto : Destination.Mastodon => (mf(masto),  Tag.Mastodon)
        case bsky  : Destination.BlueSky  => (bsf(bsky),  Tag.BlueSky)
    val headerFields =
       ujson.Obj(
         Key.version.s -> 1,
         Key.`type`.s -> tpe.toString
       )
    ujson.Obj.from(headerFields.obj ++ fields.obj)

  private def fromUJsonV1( jsonValue : ujson.Value ) : Destination =
    val obj = jsonValue.obj
    val version = obj.get(Key.version.s).map( _.num.toInt )
    if version.nonEmpty && version != Some(1) then
      throw new InvalidDestination(s"Unpickling Destination, found version ${version.get}, expected version 1: " + obj.mkString(", "))
    else
      val tpe = obj(Key.`type`.s).str
      Tag.valueOf(tpe) match
        case Tag.Email    => Destination.Email( addressPart = obj(Key.addressPart.s).str, displayNamePart = obj.get(Key.displayNamePart.s).map(_.str)) 
        case Tag.Sms      => Destination.Sms( number = obj(Key.number.s).str )
        case Tag.Mastodon => Destination.Mastodon( name = obj(Key.name.s).str, instanceUrl = obj(Key.instanceUrl.s).str )
        case Tag.BlueSky  => Destination.BlueSky( entrywayUrl = obj(Key.entrywayUrl.s).str, identifier = obj(Key.identifier.s).str )

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
  def json        : Destination.Json = Destination.Json( write[Destination]( this ) )
  def jsonPretty  : Destination.Json = Destination.Json( write[Destination]( this, indent = 4 ) )
  def toFields    : Seq[(String,String)]
  def shortDesc   : String
  def fullDesc    : String
  def defaultDesc : String
  def toCsvRow    : Seq[String]
  def whyInvalid  : Option[Throwable] = None
  def assertValid : Unit =
    whyInvalid match
      case None    => ()
      case Some(t) => throw new InvalidDestination("Invalid destination: " + this, t)

