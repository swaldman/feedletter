package com.mchange.feedletter

class FeedletterException( msg : String, cause : Throwable = null ) extends Exception( msg, cause )

class UnsupportedFeedType( msg : String, cause : Throwable = null ) extends FeedletterException( msg, cause ) 
class UntemplateNotFound( msg : String, cause : Throwable = null ) extends FeedletterException( msg, cause )
class InvalidSubscriptionType( msg : String, cause : Throwable = null ) extends FeedletterException( msg, cause )
class NoExampleItems( msg : String, cause : Throwable = null ) extends FeedletterException( msg, cause )
