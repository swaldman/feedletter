package com.mchange.feedletter.db

import com.mchange.feedletter.FeedletterException

class FeedletterDbException(msg : String, cause : Throwable = null ) extends FeedletterException(msg, cause)

class UnexpectedlyEmptyResultSet(msg : String, cause : Throwable = null ) extends FeedletterDbException(msg, cause)
class NonUniqueRow(msg : String, cause : Throwable = null )               extends FeedletterDbException(msg, cause)
class CannotUpMigrate(msg : String, cause : Throwable = null )            extends FeedletterDbException(msg, cause)
class NoRecentDump( msg : String, cause : Throwable = null )              extends FeedletterDbException(msg, cause)
