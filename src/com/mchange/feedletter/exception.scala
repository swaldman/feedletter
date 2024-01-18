package com.mchange.feedletter

class FeedletterException( msg : String, cause : Throwable = null ) extends Exception( msg, cause )

class UnsupportedFeedType( msg : String, cause : Throwable = null ) extends FeedletterException( msg, cause )
class UnsupportedUntemplateRole( msg : String, cause : Throwable = null ) extends FeedletterException( msg, cause )
class InvalidSubscriptionManager( msg : String, cause : Throwable = null ) extends FeedletterException( msg, cause )
class InvalidDestination( msg : String, cause : Throwable = null ) extends FeedletterException( msg, cause )
class ExternalApiForibidden( msg : String, cause : Throwable = null ) extends FeedletterException( msg, cause )
class AmbiguousDestination( msg : String, cause : Throwable = null ) extends FeedletterException( msg, cause )
class NoExampleItems( msg : String, cause : Throwable = null ) extends FeedletterException( msg, cause )
class WrongContentsMultiplicity( msg : String, cause : Throwable = null ) extends FeedletterException( msg, cause )
class EditorNotDefined( msg : String, cause : Throwable = null ) extends FeedletterException( msg, cause )
class InvalidEditFormat( msg : String, cause : Throwable = null ) extends FeedletterException( msg, cause )
class NoSecretSalt( msg : String, cause : Throwable = null ) extends FeedletterException( msg, cause )
class NoAccessToken( msg : String, cause : Throwable = null ) extends FeedletterException( msg, cause )
class DuplicateKey( msg : String, cause : Throwable = null ) extends FeedletterException( msg, cause )
class LeakySecrets( msg : String, cause : Throwable = null ) extends FeedletterException( msg, cause )
class WouldDropSubscriptions( msg : String, cause : Throwable = null ) extends FeedletterException( msg, cause )
