package com.mchange.feedletter

class FeedletterException( msg : String, cause : Throwable = null ) extends Exception( msg, cause )

class UnsupportedFeedType( msg : String, cause : Throwable = null ) extends FeedletterException( msg, cause ) 
class InvalidSubscriptionType( msg : String, cause : Throwable = null ) extends FeedletterException( msg, cause )
