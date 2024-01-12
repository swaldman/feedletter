package com.mchange.feedletter.api

import com.mchange.feedletter.FeedletterException

class InvalidInvitation( msg : String, cause : Throwable = null ) extends FeedletterException( msg, cause )
class InvalidRequest( msg : String, cause : Throwable = null ) extends FeedletterException( msg, cause )
class InvalidSubscribable( msg : String, cause : Throwable = null ) extends FeedletterException( msg, cause )

