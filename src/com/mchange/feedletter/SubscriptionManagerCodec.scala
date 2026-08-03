package com.mchange.feedletter

import java.time.ZoneId

import upickle.default.*

import SubscriptionManager.{BlueSky,Email,Mastodon,PeriodBased,Tag}

/**
  * Serialization mechanics for SubscriptionManager, factored out of the hierarchy itself
  * to keep SubscriptionManager.scala focused on the sealed structure.
  *
  * The `given ReadWriter[SubscriptionManager]` stays behind in `object SubscriptionManager`,
  * where upickle's implicit resolution expects to find it. Only the mechanics live here.
  *
  * Both matches below remain compiler-checked for exhaustivity from this file. Sealing
  * constrains where subtypes may be *declared*, not where they may be *matched*:
  * `toUJsonV1` is checked against the sealed SubscriptionManager hierarchy, and
  * `fromUJsonV1` against the Tag enum.
  *
  * The wire format is a persisted contract -- it lives in the `subscription_manager_json`
  * column of the `subscribable` table, and is pinned by JsonGoldenSuite. Note in particular
  * that `replyTo` and `timeZone` are omitted entirely when absent, never written as null.
  */
private[feedletter] object SubscriptionManagerCodec:

  def toUJsonV1( subscriptionManager : SubscriptionManager ) : ujson.Value =
    def esf( emailsub : Email.Instance ) : ujson.Obj = // "email shared fields"
      val values = Seq(
        "from" -> writeJs[Destination]( emailsub.from ),
        "composeUntemplateName" -> ujson.Str( emailsub.composeUntemplateName ),
        "confirmUntemplateName" -> ujson.Str( emailsub.confirmUntemplateName ),
        "statusChangeUntemplateName" -> ujson.Str( emailsub.statusChangeUntemplateName ),
        "removalNotificationUntemplateName" -> ujson.Str( emailsub.removalNotificationUntemplateName ),
        "extraParams" -> writeJs( emailsub.extraParams )
      ) ++ emailsub.replyTo.map( rt => ("replyTo" -> writeJs[Destination](rt) ) )
      ujson.Obj.from( values )
    def epbsf( pbsm : Email.Instance & PeriodBased ) : ujson.Obj = // "email period-based shared fields"
      pbsm.timeZone match
        case Some( tz ) => ujson.Obj.from( esf( pbsm ).obj addOne( ("timeZone" -> tz.getId()) ) )
        case None       => esf( pbsm )
    def eef( each : Email.Each ) : ujson.Obj = esf( each )
    def edf( daily : Email.Daily ) : ujson.Obj = epbsf( daily )
    def ewf( weekly : Email.Weekly ) : ujson.Obj = epbsf( weekly )
    def eff( fixed : Email.Fixed ) : ujson.Obj = ujson.Obj.from( esf( fixed ).obj addOne( ("numItemsPerLetter" -> fixed.numItemsPerLetter) ) )
    def maf( announce : Mastodon.Announce ) : ujson.Obj = ujson.Obj( // "mastodon announce fields"
      "extraParams" -> writeJs( announce.extraParams )
    )
    def bsf( announce : BlueSky.Announce ) : ujson.Obj = ujson.Obj(
      "extraParams" -> writeJs( announce.extraParams )
    )
    val (fields, tpe) =
      subscriptionManager match
        case each        : Email.Each        => ( eef(each),        Tag.Email_Each     )
        case daily       : Email.Daily       => ( edf(daily),       Tag.Email_Daily    )
        case weekly      : Email.Weekly      => ( ewf(weekly),      Tag.Email_Weekly   )
        case fixed       : Email.Fixed       => ( eff(fixed),       Tag.Email_Fixed    )
        case m_announce  : Mastodon.Announce => ( maf(m_announce),  Tag.Masto_Announce )
        case bs_announce : BlueSky.Announce  => ( bsf(bs_announce), Tag.Bsky_Announce  )
    val headerFields =
       ujson.Obj(
         "version" -> 1,
         "type" -> tpe.jsonVal
       )
    ujson.Obj.from(headerFields.obj ++ fields.obj)

  def fromUJsonV1( jsonValue : ujson.Value ) : SubscriptionManager =
    val obj = jsonValue.obj
    val version = obj.get("version").map( _.num.toInt )
    if version.nonEmpty && version != Some(1) then
      throw new InvalidSubscriptionManager(s"Unpickling SubscriptionManager, found version ${version.get}, expected version 1: " + obj.mkString(", "))
    else
      val tpe = obj("type").str
      val tag = Tag.forJsonVal(tpe).getOrElse:
        throw new InvalidSubscriptionManager(s"While unpickling a subscription manager, found unknown tag '$tpe': " + jsonValue)
      tag match
        case Tag.Email_Each => Email.Each(
          from = read[Destination](obj("from")).asInstanceOf[Destination.Email],
          replyTo = obj.get("replyTo").map( rtv => read[Destination](rtv).asInstanceOf[Destination.Email] ),
          composeUntemplateName             = obj("composeUntemplateName").str,
          confirmUntemplateName             = obj("confirmUntemplateName").str,
          statusChangeUntemplateName        = obj("statusChangeUntemplateName").str,
          removalNotificationUntemplateName = obj("removalNotificationUntemplateName").str,
          extraParams                       = read[Map[String,String]](obj("extraParams"))
        )
        case Tag.Email_Daily => Email.Daily(
          from = read[Destination](obj("from")).asInstanceOf[Destination.Email],
          replyTo = obj.get("replyTo").map( rtv => read[Destination](rtv).asInstanceOf[Destination.Email] ),
          composeUntemplateName             = obj("composeUntemplateName").str,
          confirmUntemplateName             = obj("confirmUntemplateName").str,
          statusChangeUntemplateName        = obj("statusChangeUntemplateName").str,
          removalNotificationUntemplateName = obj("removalNotificationUntemplateName").str,
          timeZone                          = obj.get("timeZone").map( _.str ).map( ZoneId.of ),
          extraParams                       = read[Map[String,String]](obj("extraParams"))
        )
        case Tag.Email_Weekly => Email.Weekly(
          from = read[Destination](obj("from")).asInstanceOf[Destination.Email],
          replyTo = obj.get("replyTo").map( rtv => read[Destination](rtv).asInstanceOf[Destination.Email] ),
          composeUntemplateName             = obj("composeUntemplateName").str,
          confirmUntemplateName             = obj("confirmUntemplateName").str,
          statusChangeUntemplateName        = obj("statusChangeUntemplateName").str,
          removalNotificationUntemplateName = obj("removalNotificationUntemplateName").str,
          timeZone                          = obj.get("timeZone").map( _.str ).map( ZoneId.of ),
          extraParams                       = read[Map[String,String]](obj("extraParams"))
        )
        case Tag.Email_Fixed => Email.Fixed(
          from = read[Destination](obj("from")).asInstanceOf[Destination.Email],
          replyTo = obj.get("replyTo").map( rtv => read[Destination](rtv).asInstanceOf[Destination.Email] ),
          composeUntemplateName             = obj("composeUntemplateName").str,
          confirmUntemplateName             = obj("confirmUntemplateName").str,
          statusChangeUntemplateName        = obj("statusChangeUntemplateName").str,
          removalNotificationUntemplateName = obj("removalNotificationUntemplateName").str,
          numItemsPerLetter                 = obj("numItemsPerLetter").num.toInt,
          extraParams                       = read[Map[String,String]](obj("extraParams"))
        )
        case Tag.Masto_Announce => Mastodon.Announce(
          extraParams = read[Map[String,String]](obj("extraParams"))
        )
        case Tag.Bsky_Announce => BlueSky.Announce(
          extraParams = read[Map[String,String]](obj("extraParams"))
        )

end SubscriptionManagerCodec
