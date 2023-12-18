package com.mchange.feedletter

import zio.*

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

val ComposeUntemplates         = IndexedUntemplates.filter( (k,v) => isCompose(v) )
val ComposeUntemplatesSingle   = ComposeUntemplates.filter( (k,v) => isComposeSingle(v) )
val ComposeUntemplatesMultiple = ComposeUntemplates.filter( (k,v) => isComposeMultiple(v) )

def untemplateInputType( template : Untemplate.AnyUntemplate ) : String =
  template.UntemplateInputTypeCanonical.getOrElse( template.UntemplateInputTypeDeclared )

def isCompose( candidate : Untemplate.AnyUntemplate ) : Boolean =
  candidate.UntemplateInputTypeCanonical match
    case Some( ctype ) => ctype.startsWith("com.mchange.feedletter.ComposeInfo")
    case None =>
      val checkMe  = candidate.UntemplateInputTypeDeclared
      val prefixes = "ComposeInfo" :: "com.mchange.feedletter.ComposeInfo" :: "feedletter.ComposeInfo" :: Nil
      prefixes.find( checkMe.startsWith( _ ) ).nonEmpty

def isComposeSingle( candidate : Untemplate.AnyUntemplate ) : Boolean =
  candidate.UntemplateInputTypeCanonical match
    case Some( "com.mchange.feedletter.ComposeInfo.Single" ) => true
    case Some( _ ) => false
    case None =>
      val checkMe  = candidate.UntemplateInputTypeDeclared
      val prefixes = "ComposeInfo.Single" :: "com.mchange.feedletter.ComposeInfo.Single" :: "feedletter.ComposeInfo.Single" :: Nil
      prefixes.find( checkMe == _ ).nonEmpty

def isComposeMultiple( candidate : Untemplate.AnyUntemplate ) : Boolean =
  candidate.UntemplateInputTypeCanonical match
    case Some( "com.mchange.feedletter.ComposeInfo.Multiple" ) => true
    case Some( _ ) => false
    case None =>
      val checkMe  = candidate.UntemplateInputTypeDeclared
      val prefixes = "ComposeInfo.Multiple" :: "com.mchange.feedletter.ComposeInfo.Multiple" :: "feedletter.ComposeInfo.Multiple" :: Nil
      prefixes.find( checkMe == _ ).nonEmpty

object ComposeInfo:
  sealed trait Universal:
    def feedUrl          : String
    def subscriptionName : String
    def subscriptionType : SubscriptionType
    def withinTypeId     : String
    def contents         : ItemContent | Set[ItemContent]
  end Universal  
  case class Single( feedUrl : String, subscriptionName : String, subscriptionType: SubscriptionType, withinTypeId : String, contents : ItemContent ) extends ComposeInfo.Universal
  case class Multiple( feedUrl : String, subscriptionName : String, subscriptionType: SubscriptionType, withinTypeId : String, contents : Set[ItemContent] ) extends ComposeInfo.Universal

def composeMultipleItemHtmlMailTemplate( assignableKey : AssignableKey, stype : SubscriptionType, contents : Set[ItemContent] ) : String = ???

// def composeSingleItemHtmlMailTemplate( assignableKey : AssignableKey, stype : SubscriptionType, contents : ItemContent ) : String = ???

def digestFeed( feedUrl : String ) : Task[FeedDigest] =
  ZIO.attemptBlocking( FeedDigest(feedUrl) )









