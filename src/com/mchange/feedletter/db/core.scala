package com.mchange.feedletter.db

import com.mchange.feedletter.*

import zio.*
import java.sql.*
import java.time.Instant
import javax.sql.DataSource
import scala.util.control.NonFatal
import java.lang.System

import com.mchange.cryptoutil.*

enum MetadataKey:
  case SchemaVersion
  case CreatorAppVersion

object MailSpec:
  final case class WithHash(
    seqnum       : Long,
    templateHash : Hash.SHA3_256,
    from         : AddressHeader[From],
    replyTo      : Option[AddressHeader[ReplyTo]],
    to           : AddressHeader[To],
    subject : String,
    templateParams : TemplateParams,
    retried : Int
  )
  final case class WithTemplate(
    seqnum : Long,
    templateHash : Hash.SHA3_256,
    template : String,
    from : AddressHeader[From],
    replyTo : Option[AddressHeader[ReplyTo]],
    to : AddressHeader[To],
    subject : String,
    templateParams : TemplateParams,
    retried : Int
  )



