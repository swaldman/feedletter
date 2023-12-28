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
  def forNewFeed( feedUrl : FeedUrl, minDelayMinutes : Int, awaitStabilizationMinutes : Int, maxDelayMinutes : Int, assignEveryMinutes : Int ): FeedInfo =
    val startTime = Instant.now()
    FeedInfo( None, feedUrl, minDelayMinutes, awaitStabilizationMinutes, maxDelayMinutes, assignEveryMinutes, startTime, startTime )
final case class FeedInfo( feedId : Option[FeedId], feedUrl : FeedUrl, minDelayMinutes : Int, awaitStabilizationMinutes : Int, maxDelayMinutes : Int, assignEveryMinutes : Int, added : Instant, lastAssigned : Instant ):
  def assertFeedId : FeedId = feedId.getOrElse:
    throw new FeedletterException( s"FeedInfo which should be for an extant feed, with id defined, has no feedId set: ${this}" )

final case class ExcludedItem( feedId : FeedId, guid : Guid, link : Option[String] )

final case class AdminSubscribeOptions( subscribableName : SubscribableName, destination : Destination, confirmed : Boolean, now : Instant )

def untemplateInputType( template : Untemplate.AnyUntemplate ) : String =
  template.UntemplateInputTypeCanonical.getOrElse( template.UntemplateInputTypeDeclared )

// We define this mutable(!) registry, rather than using IndexedUntemplates directly,
// because we may in future wish to define "binary" distributions that nevertheless
// permit the definition of new untemplates.
//
// Those distributions could index the untemplates under whatever name users like,
// but would need to add them to this registry at app startup.

object AllUntemplates:
  private val untemplates = scala.collection.mutable.Map.from( IndexedUntemplates )

  def add( more : IterableOnce[(String,Untemplate.AnyUntemplate)] ) : Unit = this.synchronized:
    untemplates.addAll(more)

  def get : Map[String,Untemplate.AnyUntemplate] = this.synchronized:
    Map.from( untemplates )

object TemplateParams:
  def apply( s : String ) : TemplateParams = TemplateParams( wwwFormDecodeUTF8( s ).toMap )
  val empty = TemplateParams( Map.empty )
case class TemplateParams( toMap : Map[String,String] ):
  override def toString(): String = wwwFormEncodeUTF8( toMap.toSeq* )
  def fill( template : String ) = TrivialTemplate( template ).resolve(this.toMap, TrivialTemplate.Defaults.AsIs)






