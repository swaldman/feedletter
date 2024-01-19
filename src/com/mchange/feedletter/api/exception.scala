package com.mchange.feedletter.api

import com.mchange.feedletter.FeedletterException

final class InvalidInvitation( msg : String, cause : Throwable = null ) extends FeedletterException( msg, cause )
final class InvalidRequest( msg : String, cause : Throwable = null ) extends FeedletterException( msg, cause )
final class InvalidSubscribable( msg : String, cause : Throwable = null ) extends FeedletterException( msg, cause )
final class AlreadySubscribed( msg : String, cause : Throwable = null ) extends FeedletterException( msg, cause )
