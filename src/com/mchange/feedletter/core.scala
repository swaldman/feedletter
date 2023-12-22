package com.mchange.feedletter

import zio.*
import java.lang.System

import java.nio.file.{Path as JPath}
import java.time.Instant

import javax.sql.DataSource

import scala.util.Using

import db.AssignableKey

import scala.collection.immutable

import com.mchange.conveniences.www.*
import trivialtemplate.TrivialTemplate

import untemplate.Untemplate

type ZCommand = ZIO[AppSetup & DataSource, Throwable, Any]

enum ConfigKey:
  case DumpDbDir
  case MailBatchSize
  case MailBatchDelaySeconds
  case MailMaxRetries
  case TimeZone

object SecretsKey:
  val FeedletterJdbcUrl      = "feedletter.jdbc.url"
  val FeedletterJdbcUser     = "feedletter.jdbc.user"
  val FeedletterJdbcPassword = "feedletter.jdbc.password"
  val FeedletterSecretSalt   = "feedletter.secret.salt"


type SubjectCustomizer = ( subscribableName : SubscribableName, withinTypeId : String, feedUrl : FeedUrl, contents : Set[ItemContent] ) => String
type TemplateParamCustomizer = ( subscribableName : SubscribableName, withinTypeId : String, feedUrl : FeedUrl, destination : Destination, contents : Set[ItemContent] ) => Map[String,String]

val LineSep = System.lineSeparator()

object FeedInfo:
  def forNewFeed( feedUrl : FeedUrl, minDelayMinutes : Int, awaitStabilizationMinutes : Int, maxDelayMinutes : Int ): FeedInfo =
    val startTime = Instant.now()
    FeedInfo( None, feedUrl, minDelayMinutes, awaitStabilizationMinutes, maxDelayMinutes, startTime, startTime )
final case class FeedInfo( feedId : Option[FeedId], feedUrl : FeedUrl, minDelayMinutes : Int, awaitStabilizationMinutes : Int, maxDelayMinutes : Int, added : Instant, lastAssigned : Instant ):
  def assertFeedId : FeedId = feedId.getOrElse:
    throw new FeedletterException( s"FeedInfo which should be for an extant feed, with id defined, has no feedId set: ${this}" )

final case class ExcludedItem( feedId : FeedId, guid : String, title : Option[String], author : Option[String], publicationDate : Option[Instant], link : Option[String] )

final case class AdminSubscribeOptions( subscribableName : SubscribableName, destination : Destination )

object Destination:
  def apply( s : String ) : Destination = s
opaque type Destination = String

object FeedId:
  def apply( i : Int ) : FeedId = i
opaque type FeedId = Int

extension( feedId : FeedId )
  def toInt : Int = feedId

object FeedUrl:
  def apply( s : String ) : FeedUrl = s
opaque type FeedUrl = String

object Guid:
  def apply( s : String ) : Guid = s
opaque type Guid = String

object SubscribableName:
  def apply( s : String ) : SubscribableName = s
opaque type SubscribableName = String

object TemplateParams:
  def apply( s : String ) : TemplateParams = TemplateParams( wwwFormDecodeUTF8( s ).toMap )
  //val defaults : String => String = key => s"<i>&lt;oops! could not insert param '$key'&gt;</i>" 
case class TemplateParams( toMap : Map[String,String] ):
  override def toString(): String = wwwFormEncodeUTF8( toMap.toSeq* )
  def fill( template : String ) = TrivialTemplate( template ).resolve(this.toMap, TrivialTemplate.Defaults.AsIs)

def digestFeed( feedUrl : String ) : Task[FeedDigest] =
  ZIO.attemptBlocking( FeedDigest(feedUrl) )









