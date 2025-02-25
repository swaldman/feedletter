package com.mchange.feedletter

import zio.*
import java.lang.System

import java.nio.file.{Path as JPath}
import java.time.Instant

import javax.sql.DataSource

import scala.util.Using

import scala.collection.{immutable,mutable}

import com.mchange.conveniences.www.*
import com.mchange.codegenutil.*

import trivialtemplate.TrivialTemplate

private lazy val Dingbat = s"${LineSep}-*-*-*-${LineSep}" // lazy so LineSep surely initializes first

def printSubscribable( subtup : (SubscribableName,FeedId,SubscriptionManager,Option[String]) ) : Task[Unit] =
  ZIO.attempt:
    val (sn, fi, sm, mbwti) = subtup
    println( Dingbat )
    println( s"Subscribable Name:    ${sn}" )
    println( s"Feed ID:              ${fi}" )
    mbwti.foreach: wti =>
      println( s"Last Completed Group: " + wti )
    println(   s"Subscription Manager: ${sm.jsonPretty.str}")

def printSubscribables( tups : Set[(SubscribableName,FeedId,SubscriptionManager,Option[String])] ) : Task[Unit] =
  val printOnes = tups.map( printSubscribable ).map( _ *> ZIO.attempt( println() ) )
  ZIO.collectAllDiscard( printOnes )

def printSubscriptionsCsv( subscriptionManager : SubscriptionManager, destinations : Set[Destination] ) : Task[Unit] =
  def printCsvRow( d : subscriptionManager.D ) = print( subscriptionManager.destinationCsvRow(d).mkString(",") + LineSep )
  ZIO.attempt:
    val narrowed = destinations.map( subscriptionManager.narrowDestinationOrThrow )
    print( subscriptionManager.destinationCsvRowHeaders.mkString(",") + LineSep )
    narrowed.foreach( printCsvRow )

final case class AdminSubscribeOptions( subscribableName : SubscribableName, destination : Destination, confirmed : Boolean, now : Instant )

final case class AssignableKey( subscribableName : SubscribableName, withinTypeId : String )

final case class AssignableWithinTypeStatus( withinTypeId : String, count : Int )

final case class BskyPostable( id : BskyPostableId, finalContent : String, entrywayUrl : BskyEntrywayUrl, identifier : BskyIdentifier, retried : Int, media : Seq[ItemContent.Media] )

enum ConfigKey:
  case BlueskyMaxRetries
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

final case class FeedTimings( feedId : FeedId, minDelayMinutes : Option[Int], awaitStabilizationMinutes : Option[Int], maxDelayMinutes : Option[Int], assignEveryMinutes : Option[Int] )

enum Flag:
  case ImmediateMailQueued
  case MustReloadDaemon

final case class IdentifiedDestination[T <: Destination]( subscriptionId : SubscriptionId, destination : T )

final case class ImmediateMail(
  seqnum         : Long,
  contents       : String,
  from           : AddressHeader[From],
  replyTo        : Option[AddressHeader[ReplyTo]],
  to             : AddressHeader[To],
  templateParams : TemplateParams,
  subject        : String
)

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

enum LoggingConfig:
  case User, Default

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

final case class SubscriptionInfo( id : SubscriptionId, name : SubscribableName, manager : SubscriptionManager, destination : Destination, confirmed : Boolean )

enum SubscriptionStatusChange:
  case Created, Confirmed, Removed

object TemplateParams:
  def apply( s : String ) : TemplateParams = TemplateParams( wwwFormDecodeUTF8( s ).toMap )
  val empty = TemplateParams( Map.empty )
final case class TemplateParams( toMap : Map[String,String] ):
  override def toString(): String = wwwFormEncodeUTF8( toMap.toSeq* )
  def fill( template : String ) = TrivialTemplate( template ).resolve(this.toMap, TrivialTemplate.Defaults.AsIs)

type ZCommand = ZIO[AppSetup & DataSource, Throwable, Any]




