package com.mchange.feedletter.db

import com.mchange.feedletter.{
  ConfigKey,
  FeedletterException,
  SubscriptionType
}

class FeedletterDbException(msg : String, cause : Throwable = null ) extends FeedletterException(msg, cause)

class UnexpectedlyEmptyResultSet(msg : String, cause : Throwable = null )           extends FeedletterDbException(msg, cause)
class NonUniqueRow(msg : String, cause : Throwable = null )                         extends FeedletterDbException(msg, cause)
class CannotUpMigrate(msg : String, cause : Throwable = null )                      extends FeedletterDbException(msg, cause)
class NoRecentDump( msg : String, cause : Throwable = null )                        extends FeedletterDbException(msg, cause)
class DbNotInitialized( msg : String, cause : Throwable = null )                    extends FeedletterDbException(msg, cause)
class SchemaMigrationRequired( msg : String, cause : Throwable = null )             extends FeedletterDbException(msg, cause)
class MoreRecentFeedletterVersionRequired( msg : String, cause : Throwable = null ) extends FeedletterDbException(msg, cause)
class AlreadyAssignedCantExclude( msg : String, cause : Throwable = null )          extends FeedletterDbException(msg, cause)

class ConfigurationMissing( key : ConfigKey, cause : Throwable = null ) extends FeedletterDbException(s"No ${key} configured. Please configure ${key}.", cause)

class AssignableCompleted(
  val feedUrl      : String,
  val stypeName    : String,
  val withinTypeId : String,
  val forGuid      : Option[String]
) extends FeedletterDbException(
  msg =
    s"Assignable ('${feedUrl}', '${stypeName}', '${withinTypeId}') has already been completed and cannot be further assigned to." +
    forGuid.fold("")(guid => s" (Item '${guid}' could not be included in this assignable.)")
)
