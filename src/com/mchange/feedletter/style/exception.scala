package com.mchange.feedletter.style

import com.mchange.feedletter.FeedletterException

class UntemplateNotFound( msg : String, cause : Throwable = null ) extends FeedletterException( msg, cause )
class UnsuitableUntemplate( msg : String, cause : Throwable = null ) extends FeedletterException( msg, cause )

