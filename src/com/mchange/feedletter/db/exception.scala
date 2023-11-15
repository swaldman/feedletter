package com.mchange.feedletter.db

import com.mchange.feedletter.{FeedletterException,SubscriptionType}

class FeedletterDbException(msg : String, cause : Throwable = null ) extends FeedletterException(msg, cause)

class UnexpectedlyEmptyResultSet(msg : String, cause : Throwable = null ) extends FeedletterDbException(msg, cause)
class NonUniqueRow(msg : String, cause : Throwable = null )               extends FeedletterDbException(msg, cause)
class CannotUpMigrate(msg : String, cause : Throwable = null )            extends FeedletterDbException(msg, cause)
class NoRecentDump( msg : String, cause : Throwable = null )              extends FeedletterDbException(msg, cause)


class AssignableCompleted(
  val feedUrl      : String,
  val stype        : SubscriptionType,
  val withinTypeId : String,
  val forGuid      : Option[String]
) extends FeedletterDbException(
  msg =
    s"Assignable ('${feedUrl}', '${stype}', '${withinTypeId}') has already been completed and cannot be further assigned to." +
    forGuid.fold("")(guid => s" (Item '${guid}' could not be included in this assignable.)")
)
