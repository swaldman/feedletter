package com.mchange.feedletter.db

import com.mchange.feedletter.FeedletterException

class UnexpectedlyEmptyResultSet(msg : String, cause : Throwable = null ) extends FeedletterException(msg, cause)
class NonUniqueRow(msg : String, cause : Throwable = null ) extends FeedletterException(msg, cause)
class CannotUpMigrate(msg : String, cause : Throwable = null ) extends FeedletterException(msg, cause)
