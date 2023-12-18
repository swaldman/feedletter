package com.mchange.feedletter

import zio.*

import untemplate.Untemplate

import db.AssignableKey

val ComposeUntemplates         = IndexedUntemplates.filter( (k,v) => isCompose(v) )         map ( (k,v) => (k, v.asInstanceOf[Untemplate[ComposeInfo.Universal,Nothing]]) )
val ComposeUntemplatesSingle   = ComposeUntemplates.filter( (k,v) => isComposeSingle(v) )   map ( (k,v) => (k, v.asInstanceOf[Untemplate[ComposeInfo.Single,Nothing]])    )
val ComposeUntemplatesMultiple = ComposeUntemplates.filter( (k,v) => isComposeMultiple(v) ) map ( (k,v) => (k, v.asInstanceOf[Untemplate[ComposeInfo.Multiple,Nothing]])  )

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

def serveOneHtmlPage( html : String, port : Int ) : Task[Unit] =
  import zio.http.Server
  import sttp.tapir.ztapir.*
  import sttp.tapir.server.ziohttp.ZioHttpInterpreter

  val rootEndpoint = endpoint.get.out( htmlBodyUtf8 )
  val indexEndpoint = endpoint.in("index.html").get.out( htmlBodyUtf8 )
  val logic : Unit => UIO[String] = _ => ZIO.succeed( html )
  val httpApp = ZioHttpInterpreter().toHttp( List(rootEndpoint.zServerLogic(logic), indexEndpoint.zServerLogic(logic) ) )
  Server.serve(httpApp).provide(ZLayer.succeed(Server.Config.default.port(port)), Server.live)

def serveComposeSingleUntemplate(
  untemplateName : String,
  subscriptionName : SubscribableName,
  subscriptionType : SubscriptionType,
  withinTypeId : String,
  destination : Destination,
  feedUrl : FeedUrl,
  digest : FeedDigest,
  guid : Guid,
  port : Int
) : Task[Unit] =
  val contents = digest.guidToItemContent( guid )
  val composeInfo = ComposeInfo.Single( feedUrl.toString(), subscriptionName.toString(), subscriptionType, withinTypeId, contents )
  val untemplate = ComposeUntemplatesSingle.get( untemplateName ).getOrElse:
    throw new UntemplateNotFound( s"Untemplate '${untemplateName}' seems not to be defined. Are you sure you are using the correct, fully-qualified name?" )
  val composed =
    val untemplateOutput = untemplate( composeInfo ).text
    subscriptionType match
      case templating : SubscriptionType.Templating =>
        val templateParams = templating.templateParams( subscriptionName, withinTypeId, feedUrl, destination, Set(contents) )
        templateParams.fill( untemplateOutput )
   // case _ => // this case will become relavant when some non-templating SubscriptionTypes are defines
   //   untemplateOutput 
  serveOneHtmlPage( composed, port )
