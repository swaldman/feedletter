package com.mchange.feedletter

import zio.*
import java.lang.System

import java.nio.file.{Path as JPath}
import java.time.Instant

import javax.sql.DataSource

import scala.util.Using

import db.AssignableKey

import scala.collection.{immutable,mutable}

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
  case WebDaemonPort
  case WebDaemonInterface
  case WebApiProtocol
  case WebApiHostName
  case WebApiBasePath
  case WebApiPort

object SecretsKey:
  val FeedletterJdbcUrl      = "feedletter.jdbc.url"
  val FeedletterJdbcUser     = "feedletter.jdbc.user"
  val FeedletterJdbcPassword = "feedletter.jdbc.password"
  val FeedletterSecretSalt   = "feedletter.secret.salt"

enum SubscriptionStatusChange:
  case Created, Confirmed, Removed

enum Flag:
  case MustReloadTapirApi

type SubjectCustomizer = ( subscribableName : SubscribableName, withinTypeId : String, feedUrl : FeedUrl, contents : Set[ItemContent] ) => String
type TemplateParamCustomizer = ( subscribableName : SubscribableName, withinTypeId : String, feedUrl : FeedUrl, destination : Destination, subscriptionId : SubscriptionId, removeLink : String ) => Map[String,String]

val LineSep = System.lineSeparator()

case class IdentifiedDestination[T <: Destination]( subscriptionId : SubscriptionId, destination : T )

case class SubscriptionInfo( id : SubscriptionId, name : SubscribableName, manager : SubscriptionManager, destination : Destination, confirmed : Boolean )

final case class FeedInfo( feedId : FeedId, feedUrl : FeedUrl, minDelayMinutes : Int, awaitStabilizationMinutes : Int, maxDelayMinutes : Int, assignEveryMinutes : Int, added : Instant, lastAssigned : Instant )

object NascentFeed:
  def apply( feedUrl : FeedUrl, minDelayMinutes : Int, awaitStabilizationMinutes : Int, maxDelayMinutes : Int, assignEveryMinutes : Int ) : NascentFeed =
    val now = Instant.now
    NascentFeed( feedUrl, minDelayMinutes, awaitStabilizationMinutes, maxDelayMinutes, assignEveryMinutes, now, now )
final case class NascentFeed( feedUrl : FeedUrl, minDelayMinutes : Int, awaitStabilizationMinutes : Int, maxDelayMinutes : Int, assignEveryMinutes : Int, added : Instant, lastAssigned : Instant )

final case class ExcludedItem( feedId : FeedId, guid : Guid, link : Option[String] )

final case class AdminSubscribeOptions( subscribableName : SubscribableName, destination : Destination, confirmed : Boolean, now : Instant )

def untemplateInputType( template : Untemplate.AnyUntemplate ) : String =
  template.UntemplateInputTypeCanonical.getOrElse( template.UntemplateInputTypeDeclared )

object TemplateParams:
  def apply( s : String ) : TemplateParams = TemplateParams( wwwFormDecodeUTF8( s ).toMap )
  val empty = TemplateParams( Map.empty )
case class TemplateParams( toMap : Map[String,String] ):
  override def toString(): String = wwwFormEncodeUTF8( toMap.toSeq* )
  def fill( template : String ) = TrivialTemplate( template ).resolve(this.toMap, TrivialTemplate.Defaults.AsIs)







