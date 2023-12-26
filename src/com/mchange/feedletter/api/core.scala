package com.mchange.feedletter.api

import com.mchange.feedletter.Destination

import com.mchange.cryptoutil.*

object RequestPayload:
  object Subscription:
    case class Request( subscribableId : Int, destination : Destination ) extends RequestPayload
    case class Confirm( subscriptionId : Long, invitation : String ) extends RequestPayload.Invited:
      def unsaltedBytes : Seq[Byte] = subscriptionId.toByteSeqBigEndian ++ invitation.decodeHexToSeq
  trait Invited extends RequestPayload:
    def invitation    : String
    def unsaltedBytes : Seq[Byte]
trait RequestPayload
