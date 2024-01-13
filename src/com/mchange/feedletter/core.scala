package com.mchange.feedletter

import zio.*
import java.lang.System

import java.nio.file.{Path as JPath}
import java.time.Instant

import javax.sql.DataSource

import scala.util.Using

import scala.collection.{immutable,mutable}

import com.mchange.conveniences.www.*
import trivialtemplate.TrivialTemplate

final case class AdminSubscribeOptions( subscribableName : SubscribableName, destination : Destination, confirmed : Boolean, now : Instant )

final case class AssignableKey( subscribableName : SubscribableName, withinTypeId : String )

final case class AssignableWithinTypeStatus( withinTypeId : String, count : Int )

enum ConfigKey:
  case ConfirmHours
  case DumpDbDir
  case MailBatchSize
  case MailBatchDelaySeconds
  case MailMaxRetries
  case MastodonMaxRetries
  case TimeZone
  case WebDaemonPort
  case WebDaemonInterface
  case WebApiProtocol
  case WebApiHostName
  case WebApiBasePath
  case WebApiPort

final case class ExcludedItem( feedId : FeedId, guid : Guid, link : Option[String] )

final case class FeedInfo( feedId : FeedId, feedUrl : FeedUrl, minDelayMinutes : Int, awaitStabilizationMinutes : Int, maxDelayMinutes : Int, assignEveryMinutes : Int, added : Instant, lastAssigned : Instant )

enum Flag:
  case MustReloadDaemon

final case class IdentifiedDestination[T <: Destination]( subscriptionId : SubscriptionId, destination : T )

/*
 * Maps directly to a postgres enum,
 * if this is changed, keep that in sync
 */
enum ItemAssignability:
  case Unassigned
  case Assigned
  case Cleared
  case Excluded

final case class ItemStatus( contentHash : Int, firstSeen : Instant, lastChecked : Instant, stableSince : Instant, assignability : ItemAssignability )

val LineSep = System.lineSeparator()

type MastoAnnouncementCustomizer = ( subscribableName : SubscribableName, subscriptionManager : SubscriptionManager, withinTypeId : String, feedUrl : FeedUrl, content : ItemContent ) => Option[String]

final case class MastoPostable( id : MastoPostableId, finalContent : String, instanceUrl : MastoInstanceUrl, name : MastoName, retried : Int, media : Seq[ItemContent.Media] )

object NascentFeed:
  def apply( feedUrl : FeedUrl, minDelayMinutes : Int, awaitStabilizationMinutes : Int, maxDelayMinutes : Int, assignEveryMinutes : Int ) : NascentFeed =
    val now = Instant.now
    NascentFeed( feedUrl, minDelayMinutes, awaitStabilizationMinutes, maxDelayMinutes, assignEveryMinutes, now, now )
final case class NascentFeed( feedUrl : FeedUrl, minDelayMinutes : Int, awaitStabilizationMinutes : Int, maxDelayMinutes : Int, assignEveryMinutes : Int, added : Instant, lastAssigned : Instant )

object SecretsKey:
  val FeedletterJdbcUrl      = "feedletter.jdbc.url"
  val FeedletterJdbcUser     = "feedletter.jdbc.user"
  val FeedletterJdbcPassword = "feedletter.jdbc.password"
  val FeedletterSecretSalt   = "feedletter.secret.salt"

type SubjectCustomizer = ( subscribableName : SubscribableName, subscriptionManager : SubscriptionManager, withinTypeId : String, feedUrl : FeedUrl, contents : Set[ItemContent] ) => String

final case class SubscriptionInfo( id : SubscriptionId, name : SubscribableName, manager : SubscriptionManager, destination : Destination, confirmed : Boolean )

enum SubscriptionStatusChange:
  case Created, Confirmed, Removed

type TemplateParamCustomizer =
  ( subscribableName : SubscribableName, subscriptionManager : SubscriptionManager, withinTypeId : String, feedUrl : FeedUrl, destination : Destination, subscriptionId : SubscriptionId, removeLink : String ) => Map[String,String]

object TemplateParams:
  def apply( s : String ) : TemplateParams = TemplateParams( wwwFormDecodeUTF8( s ).toMap )
  val empty = TemplateParams( Map.empty )
final case class TemplateParams( toMap : Map[String,String] ):
  override def toString(): String = wwwFormEncodeUTF8( toMap.toSeq* )
  def fill( template : String ) = TrivialTemplate( template ).resolve(this.toMap, TrivialTemplate.Defaults.AsIs)

type ZCommand = ZIO[AppSetup & DataSource, Throwable, Any]




