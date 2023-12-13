package com.mchange.feedletter.db

import java.sql.{Connection,Statement,Timestamp,Types}
import java.time.Instant
import scala.util.Using

import com.mchange.feedletter.{ConfigKey,Destination,ExcludedItem,FeedInfo,FeedUrl,Guid,InvalidSubscriptionType,ItemContent,SubscribableName,SubscriptionType,TemplateParams}

import com.mchange.cryptoutil.{Hash, given}

object PgSchema:
  trait Creatable:
    protected def Create : String
    def create( stmt : Statement ) : Int = stmt.executeUpdate( this.Create )
    def create( conn : Connection ) : Int = Using.resource( conn.createStatement() )( stmt => create(stmt) )

  object Unversioned:
    object Table:
      object Metadata extends Creatable:
        val Name = "metadata"
        protected val Create = "CREATE TABLE metadata( key VARCHAR(64) PRIMARY KEY, value VARCHAR(64) NOT NULL )"
        private val Insert = "INSERT INTO metadata(key, value) VALUES( ?, ? )"
        private val Update = "UPDATE metadata SET value = ? WHERE key = ?"
        private val Select = "SELECT value FROM metadata WHERE key = ?"
        def insert( conn : Connection, key : MetadataKey, value : String ) : Int =
          Using.resource( conn.prepareStatement( this.Insert ) ): ps =>
            ps.setString( 1, key.toString() )
            ps.setString( 2, value )
            ps.executeUpdate()
        def update( conn : Connection, key : MetadataKey, newValue : String ) : Int =
          Using.resource( conn.prepareStatement(this.Update) ): ps =>
            ps.setString(1, newValue)
            ps.setString(2, key.toString())
            ps.executeUpdate()
        def select( conn : Connection, key : MetadataKey ) : Option[String] =
          Using.resource( conn.prepareStatement( this.Select ) ): ps =>
            ps.setString(1, key.toString())
            Using.resource( ps.executeQuery() ): rs =>
              zeroOrOneResult("select-metadata", rs)( _.getString(1) )

  trait Base:
    def Version : Int
  object V0 extends Base: // contains unversioned schema only
    override val Version = 0
  object V1 extends Base:
    override val Version = 1
      object Table:
        object Config extends Creatable:
          protected val Create = "CREATE TABLE config( key VARCHAR(1024) PRIMARY KEY, value VARCHAR(1024) NOT NULL )"
          private val Insert = "INSERT INTO config(key, value) VALUES( ?, ? )"
          private val Update = "UPDATE config SET value = ? WHERE key = ?"
          private val Select = "SELECT value FROM config WHERE key = ?"
          private val SelectTuples = "SELECT key, value FROM config"
          private val Upsert =
            """|INSERT INTO config(key, value)
               |VALUES ( ?, ? )
               |ON CONFLICT(key) DO UPDATE
               |SET value = ?""".stripMargin
          def insert( conn : Connection, key : ConfigKey, value : String ) : Int =
            Using.resource( conn.prepareStatement( this.Insert ) ): ps =>
              ps.setString( 1, key.toString() )
              ps.setString( 2, value )
              ps.executeUpdate()
          def update( conn : Connection, key : ConfigKey, newValue : String ) : Int =
            Using.resource( conn.prepareStatement(this.Update) ): ps =>
              ps.setString(1, newValue)
              ps.setString(2, key.toString())
              ps.executeUpdate()
          def select( conn : Connection, key : ConfigKey ) : Option[String] =
            Using.resource( conn.prepareStatement( this.Select ) ): ps =>
              ps.setString(1, key.toString())
              Using.resource( ps.executeQuery() ): rs =>
                zeroOrOneResult("select-config-item", rs)( _.getString(1) )
          def upsert( conn : Connection, key : ConfigKey, value : String ) =
            Using.resource( conn.prepareStatement( this.Upsert ) ): ps =>
              ps.setString(1, key.toString())
              ps.setString(2, value )
              ps.setString(3, value )
              ps.executeUpdate()
          def selectTuples( conn : Connection ) : Set[Tuple2[ConfigKey,String]] =
            Using.resource( conn.prepareStatement( this.SelectTuples ) ): ps =>
              Using.resource( ps.executeQuery() ): rs =>
                toSet(rs)( rs => Tuple2( ConfigKey.valueOf( rs.getString(1) ), rs.getString(2) ) )

        object Feed extends Creatable:
          protected val Create =
            """|CREATE TABLE feed(
               |  url                         VARCHAR(1024),
               |  min_delay_minutes           INTEGER NOT NULL,
               |  await_stabilization_minutes INTEGER NOT NULL,
               |  max_delay_minutes           INTEGER NOT NULL,
               |  subscribed                  TIMESTAMP NOT NULL,
               |  last_assigned               TIMESTAMP NOT NULL,     -- we'll start at subscribed
               |  PRIMARY KEY(url)
               |)""".stripMargin
          private val Insert =
            """|INSERT INTO feed(url, min_delay_minutes, await_stabilization_minutes, max_delay_minutes, subscribed, last_assigned)
               |VALUES( ?, ?, ?, ?, ?, ? )""".stripMargin
          private val SelectAll =
            "SELECT url, min_delay_minutes, await_stabilization_minutes, max_delay_minutes, subscribed, last_assigned FROM feed"
          private val SelectLastAssigned =
            """|SELECT last_assigned
               |FROM feed
               |WHERE url = ?""".stripMargin
          private val Upsert =
            """|INSERT INTO feed(url, min_delay_minutes, await_stabilization_minutes, max_delay_minutes, subscribed, last_assigned)
               |VALUES ( ?, ?, ?, ?, ?, ? )
               |ON CONFLICT(url) DO UPDATE
               |SET min_delay_minutes = ?, await_stabilization_minutes = ?, max_delay_minutes = ? """.stripMargin // leave subscribed and last_assigned unchange
          private def UpdateLastAssigned =
            """|UPDATE feed
               |SET last_assigned = ?
               |WHERE url = ?""".stripMargin
          def insert( conn : Connection, fi : FeedInfo ) : Int =
            insert(conn, fi.feedUrl, fi.minDelayMinutes, fi.awaitStabilizationMinutes, fi.maxDelayMinutes, fi.subscribed, fi.lastAssigned)
          def insert( conn : Connection, feedUrl : FeedUrl, minDelayMinutes : Int, awaitStabilizationMinutes : Int, maxDelayMinutes : Int, subscribed : Instant, lastAssigned : Instant ) : Int =
            Using.resource(conn.prepareStatement(this.Insert)): ps =>
              ps.setString    (1, feedUrl.toString())
              ps.setInt       (2, minDelayMinutes )
              ps.setInt       (3, awaitStabilizationMinutes)
              ps.setInt       (4, maxDelayMinutes )
              ps.setTimestamp (5, Timestamp.from(subscribed) )
              ps.setTimestamp (6, Timestamp.from(lastAssigned) )
              ps.executeUpdate()
          def selectAll( conn : Connection ) : Set[FeedInfo] =
            Using.resource( conn.prepareStatement( this.SelectAll ) ): ps =>
              Using.resource( ps.executeQuery() ): rs =>
                toSet(rs)( rs => FeedInfo( FeedUrl( rs.getString(1) ), rs.getInt(2), rs.getInt(3), rs.getInt(4), rs.getTimestamp(5).toInstant, rs.getTimestamp(6).toInstant ) )
          def selectLastAssigned( conn : Connection, feedUrl : FeedUrl ) : Option[Instant] =
            Using.resource(conn.prepareStatement(this.SelectLastAssigned)): ps =>
              ps.setString(1, feedUrl.toString())
              Using.resource( ps.executeQuery() ): rs =>
                zeroOrOneResult("select-when-feed-last-assigned", rs)( _.getTimestamp(1).toInstant() )

          def upsert( conn : Connection, fi : FeedInfo ) =
            Using.resource( conn.prepareStatement(this.Upsert) ): ps =>
              ps.setString    (1, fi.feedUrl.toString())
              ps.setInt       (2, fi.minDelayMinutes )
              ps.setInt       (3, fi.awaitStabilizationMinutes)
              ps.setInt       (4, fi.maxDelayMinutes )
              ps.setTimestamp (5, Timestamp.from(fi.subscribed))
              ps.setTimestamp (6, Timestamp.from(fi.lastAssigned))
              ps.setInt       (7, fi.minDelayMinutes )
              ps.setInt       (8, fi.awaitStabilizationMinutes)
              ps.setInt       (9, fi.maxDelayMinutes )
              ps.executeUpdate()
          def updateLastAssigned( conn : Connection, feedUrl : FeedUrl, lastAssigned : Instant ) =
            Using.resource( conn.prepareStatement(this.UpdateLastAssigned) ): ps =>
              ps.setTimestamp(1, Timestamp.from(lastAssigned))
              ps.setString   (2, feedUrl.toString())
              ps.executeUpdate()

        object Item extends Creatable:
          object Type:
            object ItemAssignability extends Creatable:
              protected val Create = "CREATE TYPE ItemAssignability AS ENUM ('Unassigned', 'Assigned', 'Excluded')"
          protected val Create =
            """|CREATE TABLE item(
               |  feed_url         VARCHAR(1024),
               |  guid             VARCHAR(1024),
               |  title            VARCHAR(1024),
               |  author           VARCHAR(1024),
               |  article          TEXT,
               |  publication_date TIMESTAMP,
               |  link             VARCHAR(1024),
               |  content_hash     INTEGER NOT NULL,   -- ItemContent.## (hashCode)
               |  first_seen       TIMESTAMP NOT NULL,
               |  last_checked     TIMESTAMP NOT NULL,
               |  stable_since     TIMESTAMP NOT NULL,
               |  assignability    ItemAssignability NOT NULL,
               |  PRIMARY KEY(feed_url, guid),
               |  FOREIGN KEY(feed_url) REFERENCES feed(url)
               |)""".stripMargin
          private val SelectCheck =
            """|SELECT content_hash, first_seen, last_checked, stable_since, assignability
               |FROM item
               |WHERE feed_url = ? AND guid = ?""".stripMargin
          private val SelectExcluded =
            s"""|SELECT feed_url, guid, title, author, publication_date, link
                |FROM item
                |WHERE assignability = '${ItemAssignability.Excluded}'""".stripMargin
          private val Insert =
            """|INSERT INTO item(feed_url, guid, title, author, article, publication_date, link, content_hash, first_seen, last_checked, stable_since, assignability)
               |VALUES( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CAST( ? AS ItemAssignability ) )""".stripMargin
          private val UpdateChanged =
            """|UPDATE item
               |SET title = ?, author = ?, article = ?, publication_date = ?, link = ?, content_hash = ?, last_checked = ?, stable_since = ?, assignability = CAST( ? AS ItemAssignability )
               |WHERE feed_url = ? AND guid = ?""".stripMargin
          private val UpdateStable =
            """|UPDATE item
               |SET last_checked = ?
               |WHERE feed_url = ? AND guid = ?""".stripMargin
          private val UpdateAssignability =
            """|UPDATE item
               |SET assignability = CAST( ? AS ItemAssignability )
               |WHERE feed_url = ? AND guid = ?""".stripMargin
          def checkStatus( conn : Connection, feedUrl : FeedUrl, guid : Guid ) : Option[ItemStatus] =
            Using.resource( conn.prepareStatement( SelectCheck ) ): ps =>
              ps.setString(1, feedUrl.toString())
              ps.setString(2, guid.toString())
              Using.resource( ps.executeQuery() ): rs =>
                zeroOrOneResult("item-check-select", rs): rs =>
                  ItemStatus( rs.getInt(1), rs.getTimestamp(2).toInstant(), rs.getTimestamp(3).toInstant(), rs.getTimestamp(4).toInstant(), ItemAssignability.valueOf(rs.getString(5)) )
          def selectExcluded( conn : Connection ) : Set[ExcludedItem] =
            Using.resource( conn.prepareStatement(SelectExcluded) ): ps =>
              Using.resource( ps.executeQuery() ): rs =>
                toSet(rs)( rs => ExcludedItem( FeedUrl( rs.getString(1) ), rs.getString(2), Option( rs.getString(3) ), Option( rs.getString(4) ), Option( rs.getTimestamp(5) ).map( _.toInstant ), Option( rs.getString(6) ) ) )
          def updateStable( conn : Connection, feedUrl : FeedUrl, guid : Guid, lastChecked : Instant ) =
            Using.resource( conn.prepareStatement( this.UpdateStable) ): ps =>
              ps.setTimestamp(1, Timestamp.from(lastChecked))
              ps.setString(2, feedUrl.toString())
              ps.setString(3, guid.toString())
              ps.executeUpdate()
          def updateChanged( conn : Connection, feedUrl : FeedUrl, guid : Guid, newContent : ItemContent, newStatus : ItemStatus ) =
            Using.resource( conn.prepareStatement( this.UpdateChanged ) ): ps =>
              setStringOptional(ps, 1, Types.VARCHAR, newContent.title )
              setStringOptional(ps, 2, Types.VARCHAR, newContent.author )
              setStringOptional(ps, 3, Types.CLOB, newContent.article )
              setTimestampOptional(ps, 4, newContent.pubDate.map( Timestamp.from ))
              setStringOptional(ps, 5, Types.VARCHAR, newContent.link)
              ps.setInt(6, newStatus.contentHash)
              ps.setTimestamp(7, Timestamp.from(newStatus.lastChecked))
              ps.setTimestamp(8, Timestamp.from(newStatus.stableSince))
              ps.setString(9, newStatus.assignability.toString())
              ps.setString(10, feedUrl.toString())
              ps.setString(11, guid.toString())
              ps.executeUpdate()
          def updateAssignability( conn : Connection, feedUrl : FeedUrl, guid : Guid, assignability : ItemAssignability ) =
            Using.resource( conn.prepareStatement( this.UpdateAssignability) ): ps =>
              ps.setString(1, assignability.toString())
              ps.setString(2, feedUrl.toString())
              ps.setString(3, guid.toString())
              ps.executeUpdate()
          def insertNew( conn : Connection, feedUrl : FeedUrl, guid : Guid, itemContent : ItemContent, assignability : ItemAssignability ) : Int =
            Using.resource( conn.prepareStatement( Insert ) ): ps =>
              val now = Instant.now
              ps.setString( 1, feedUrl.toString() )
              ps.setString( 2, guid.toString() )
              setStringOptional(ps, 3, Types.VARCHAR, itemContent.title)
              setStringOptional(ps, 4, Types.VARCHAR, itemContent.author)
              setStringOptional(ps, 5, Types.CLOB, itemContent.article)
              setTimestampOptional(ps, 6, itemContent.pubDate.map( Timestamp.from ))
              setStringOptional(ps, 7, Types.VARCHAR, itemContent.link)
              ps.setInt( 8, itemContent.## )
              ps.setTimestamp( 9, Timestamp.from( now ) )
              ps.setTimestamp( 10, Timestamp.from( now ) )
              ps.setTimestamp( 11, Timestamp.from( now ) )
              ps.setString( 12, assignability.toString() )
              ps.executeUpdate()
        object Subscribable extends Creatable:
          protected val Create =
            """|CREATE TABLE subscribable(
               |  feed_url          VARCHAR(1024),
               |  subscribable_name VARCHAR(64),
               |  subscription_type TEXT,
               |  PRIMARY KEY (feed_url, subscribable_name),
               |  FOREIGN KEY (feed_url) REFERENCES feed(url)
               |)""".stripMargin
          private val Select = "SELECT feed_url, subscribable_name, subscription_type FROM subscribable"
          private val SelectType =
            """|SELECT subscription_type
               |FROM subscribable
               |WHERE feed_url = ? AND subscribable_name = ?""".stripMargin
          private val Insert = "INSERT INTO subscribable VALUES ( ?, ?, ? )"
          def select( conn : Connection ) : Set[(FeedUrl, SubscribableName, SubscriptionType)] =
            Using.resource( conn.prepareStatement( Select ) ): ps =>
              Using.resource( ps.executeQuery() ): rs =>
                toSet(rs)( rs => ( FeedUrl( rs.getString(1) ), SubscribableName( rs.getString(2) ), SubscriptionType.parse( rs.getString(3) ) ) )
          def selectType( conn : Connection, feedUrl : FeedUrl, subscribableName : SubscribableName ) : SubscriptionType =
            Using.resource( conn.prepareStatement( SelectType ) ): ps =>
              ps.setString(1, feedUrl.toString())
              ps.setString(2, subscribableName.toString())
              Using.resource( ps.executeQuery() ): rs =>
                uniqueResult("select-subscription-type", rs)( rs => SubscriptionType.parse( rs.getString(1) ) )
          def insert( conn : Connection, feedUrl : FeedUrl, subscribableName : SubscribableName, subscriptionType : SubscriptionType ) =
            Using.resource( conn.prepareStatement( Insert ) ): ps =>
              ps.setString(1, feedUrl.toString())
              ps.setString(2, subscribableName.toString())
              ps.setString(3, subscriptionType.toString())
              ps.executeUpdate()
        object Assignable extends Creatable:
          protected val Create = // an assignable represents a collection of posts for a single mail
            """|CREATE TABLE assignable(
               |  feed_url          VARCHAR(1024),
               |  subscribable_name VARCHAR(64),
               |  within_type_id    VARCHAR(1024),
               |  opened            TIMESTAMP NOT NULL,
               |  completed         TIMESTAMP,
               |  PRIMARY KEY(feed_url, subscribable_name, within_type_id),
               |  FOREIGN KEY(feed_url, subscribable_name) REFERENCES subscribable(feed_url, subscribable_name)
               |)""".stripMargin
          private val SelectIsCompleted =
            """|SELECT completed IS NOT NULL
               |FROM assignable
               |WHERE feed_url = ? AND subscribable_name = ? AND within_type_id = ?""".stripMargin
          private val SelectOpen =
            """|SELECT feed_url, subscribable_name, within_type_id
               |FROM assignable
               |WHERE completed IS NULL""".stripMargin
          // should I make indexes for these next two? should I try some more clever/efficient form of query?
          // see
          //   https://stackoverflow.com/questions/3800551/select-first-row-in-each-group-by-group/7630564#7630564
          //   https://stackoverflow.com/questions/1684244/efficient-latest-record-query-with-postgresql
          //   https://www.timescale.com/blog/select-the-most-recent-record-of-many-items-with-postgresql/
          private val SelectWithinTypeIdMostRecentOpen =
            """|SELECT within_type_id
               |FROM assignable
               |WHERE feed_url = ? AND subscribable_name = ?
               |ORDER BY opened DESC
               |LIMIT 1""".stripMargin
          private val SelectWithinTypeIdLastCompleted =
            """|SELECT within_type_id
               |FROM assignable
               |WHERE feed_url = ? AND subscribable_name = ?
               |ORDER BY completed DESC NULLS LAST
               |LIMIT 1""".stripMargin
          private val Insert =
            """|INSERT INTO assignable( feed_url, subscribable_name, within_type_id, opened, completed )
               |VALUES ( ?, ?, ?, ?, ? )""".stripMargin
          private val UpdateCompleted =
            """|UPDATE assignable
               |SET completed = ?
               |WHERE feed_url = ? AND subscribable_name = ? AND within_type_id = ?""".stripMargin
          def selectOpen( conn : Connection ) : Set[AssignableKey] =
            Using.resource( conn.prepareStatement( this.SelectOpen ) ): ps =>
              Using.resource( ps.executeQuery() ): rs =>
                toSet(rs)( rs => AssignableKey( FeedUrl( rs.getString(1) ), SubscribableName( rs.getString(2) ), rs.getString(3) ) )
          def selectIsCompleted( conn : Connection, feedUrl : FeedUrl, subscribableName : SubscribableName, withinTypeId : String ) : Option[Boolean] =
            Using.resource( conn.prepareStatement( this.SelectIsCompleted ) ): ps =>
              ps.setString(1, feedUrl.toString())
              ps.setString(2, subscribableName.toString())
              ps.setString(3, withinTypeId)
              Using.resource(ps.executeQuery()): rs =>
                zeroOrOneResult("assignable-select-completed", rs)( _.getBoolean(1) )
          def selectWithinTypeIdMostRecentOpen( conn : Connection, feedUrl : FeedUrl, subscribableName : SubscribableName ) : Option[String] =
            Using.resource( conn.prepareStatement( this.SelectWithinTypeIdMostRecentOpen ) ): ps =>
              ps.setString(1, feedUrl.toString())
              ps.setString(2, subscribableName.toString())
              Using.resource(ps.executeQuery()): rs =>
                zeroOrOneResult("assignable-select-most-recent-open-within-type-id", rs)( _.getString(1) )
          def selectWithinTypeIdLastCompleted( conn : Connection, feedUrl : FeedUrl, subscribableName : SubscribableName ) : Option[String] =
            Using.resource( conn.prepareStatement( this.SelectWithinTypeIdLastCompleted ) ): ps =>
              ps.setString(1, feedUrl.toString())
              ps.setString(2, subscribableName.toString())
              Using.resource(ps.executeQuery()): rs =>
                zeroOrOneResult("assignable-select-last-completed-within-type-id", rs)( _.getString(1) )
          def insert( conn : Connection, feedUrl : FeedUrl, subscribableName : SubscribableName, withinTypeId : String, opened : Instant, completed : Option[Instant] ) =
            Using.resource( conn.prepareStatement( this.Insert ) ): ps =>
              ps.setString(1, feedUrl.toString())
              ps.setString(2, subscribableName.toString())
              ps.setString(3, withinTypeId)
              ps.setTimestamp(4, Timestamp.from(opened))
              setTimestampOptional(ps, 5, completed.map( Timestamp.from ))
              ps.executeUpdate()
          def updateCompleted( conn : Connection, feedUrl : FeedUrl, subscribableName : SubscribableName, withinTypeId : String, completed : Option[Instant] ) =
            Using.resource( conn.prepareStatement( this.UpdateCompleted ) ): ps =>
              completed.fold( ps.setNull(1, Types.TIMESTAMP) )( instant => ps.setTimestamp(1, Timestamp.from(instant)) )
              ps.setString(2, feedUrl.toString())
              ps.setString(3, subscribableName.toString())
              ps.setString(4, withinTypeId)
              ps.executeUpdate()
        object Assignment extends Creatable:
          protected val Create = // an assignment represents a membership of a post in a collection
            """|CREATE TABLE assignment(
               |  feed_url          VARCHAR(1024),
               |  subscribable_name VARCHAR(64),
               |  within_type_id    VARCHAR(1024),
               |  guid              VARCHAR(1024),
               |  PRIMARY KEY( feed_url, subscribable_name, within_type_id, guid ),
               |  FOREIGN KEY( feed_url, guid ) REFERENCES item( feed_url, guid ),
               |  FOREIGN KEY( feed_url, subscribable_name, within_type_id ) REFERENCES assignable( feed_url, subscribable_name, within_type_id )
               |)""".stripMargin
          private val SelectCountWithinAssignable =
            """|SELECT COUNT(*)
               |FROM assignment
               |WHERE feed_url = ? AND subscribable_name = ? AND within_type_id = ?""".stripMargin
          private val Insert =
            """|INSERT INTO assignment( feed_url, subscribable_name, within_type_id, guid )
               |VALUES ( ?, ?, ?, ? )""".stripMargin
          def selectCountWithinAssignable( conn : Connection, feedUrl : FeedUrl, subscribableName : SubscribableName, withinTypeId : String ) : Int =
            Using.resource( conn.prepareStatement( this.SelectCountWithinAssignable ) ): ps =>
              ps.setString(1, feedUrl.toString())
              ps.setString(2, subscribableName.toString())
              ps.setString(3, withinTypeId)
              Using.resource( ps.executeQuery() ): rs =>
                uniqueResult( "select-count-within-assignable", rs )( _.getInt(1) )
          def insert( conn : Connection, feedUrl : FeedUrl, subscribableName : SubscribableName, withinTypeId : String, guid : Guid ) =
            Using.resource( conn.prepareStatement( this.Insert ) ): ps =>
              ps.setString(1, feedUrl.toString())
              ps.setString(2, subscribableName.toString())
              ps.setString(3, withinTypeId)
              ps.setString(4, guid.toString())
              ps.executeUpdate()
        object Subscription extends Creatable:
          protected val Create =
            """|CREATE TABLE subscription(
               |  destination       VARCHAR(1024),
               |  feed_url          VARCHAR(1024),
               |  subscribable_name VARCHAR(64),
               |  PRIMARY KEY( destination, feed_url, subscribable_name ),
               |  FOREIGN KEY( feed_url, subscribable_name ) REFERENCES subscribable( feed_url, subscribable_name )
               |)""".stripMargin
          private val SelectSubscriptionTypeNamesByFeedUrl =
            """|SELECT DISTINCT subscribable_name
               |FROM subscription
               |WHERE feed_url = ?""".stripMargin
          private val SelectDestination =
            """|SELECT destination
               |FROM subscription
               |WHERE feed_url = ? AND subscribable_name = ?""".stripMargin
          private val Insert =
            """|INSERT INTO subscription(destination, feed_url, subscribable_name)
               |VALUES ( ?, ?, ? )""".stripMargin
          def selectSubscriptionTypeNamesByFeedUrl( conn : Connection, feedUrl : FeedUrl ) : Set[SubscribableName] =
            Using.resource( conn.prepareStatement( this.SelectSubscriptionTypeNamesByFeedUrl ) ): ps =>
              ps.setString(1, feedUrl.toString())
              Using.resource( ps.executeQuery() ): rs =>
                toSet(rs)( rs => SubscribableName( rs.getString(1) ) )
          def selectDestination( conn : Connection, feedUrl : FeedUrl, subscribableName : SubscribableName ) : Set[Destination] =
            Using.resource( conn.prepareStatement( this.SelectDestination ) ): ps =>
              ps.setString(1, feedUrl.toString())
              ps.setString(2, subscribableName.toString())
              Using.resource( ps.executeQuery() ): rs =>
                toSet(rs)( rs => Destination( rs.getString(1) ) )
          def insert( conn : Connection, destination : Destination, feedUrl : FeedUrl, subscribableName : SubscribableName ) =
            Using.resource( conn.prepareStatement( Insert ) ): ps =>
              ps.setString(1, destination.toString())
              ps.setString(2, feedUrl.toString())
              ps.setString(3, subscribableName.toString())
              ps.executeUpdate()

        // publication-related tables should be decoupled from, unrelated to the
        // tables above. logically, we should be listening for "completion" above
        // as a kind of event, which would trigger SubscriptionType route potentially
        // in a distinct process with a distinct database
        object MailableTemplate extends Creatable:
          protected val Create =
            """|CREATE TABLE mailable_template(
               |  sha3_256 CHAR(64),
               |  template TEXT,
               |  PRIMARY KEY(sha3_256)
               |)""".stripMargin
          private val Insert =
            """|INSERT INTO mailable_template(sha3_256, template)
               |VALUES ( ?, ? )""".stripMargin
          private val Ensure =
            """|INSERT INTO mailable_template(sha3_256, template)
               |VALUES ( ?, ? )
               |ON CONFLICT(sha3_256) DO NOTHING""".stripMargin
          private val SelectByHash =
            """|SELECT template
               |FROM mailable_template
               |WHERE sha3_256 = ?""".stripMargin
          def ensure( conn : Connection, hash : Hash.SHA3_256, template : String ) =
            Using.resource( conn.prepareStatement(Ensure) ): ps =>
              ps.setString(1, hash.hex )
              ps.setString(2, template )
              ps.executeUpdate()
          def selectByHash( conn : Connection, hash : Hash.SHA3_256 ) : Option[String] =
            Using.resource( conn.prepareStatement( SelectByHash ) ): ps =>
              ps.setString( 1, hash.hex )
              Using.resource( ps.executeQuery() ): rs =>
                zeroOrOneResult("select-mailable-template-by-hash",rs)( _.getString(1) )
        object Mailable extends Creatable:
          protected val Create =
            """|CREATE TABLE mailable(
               |  seqnum          BIGINT,
               |  sha3_256        CHAR(64) NOT NULL,
               |  mail_from       VARCHAR(256) NOT NULL,
               |  mail_reply_to   VARCHAR(256),           -- mail_reply_to could be NULL!
               |  mail_to         VARCHAR(256) NOT NULL,
               |  mail_subject    VARCHAR(256) NOT NULL,
               |  template_params TEXT,                   -- www-form-encoded
               |  retried         INTEGER NOT NULL,
               |  PRIMARY KEY(seqnum),
               |  FOREIGN KEY(sha3_256) REFERENCES mailable_template(sha3_256)
               |)""".stripMargin
          private val SelectForDelivery =
            """|SELECT seqnum, sha3_256, mail_from, mail_reply_to, mail_to, mail_subject, template_params, retried
               |FROM mailable
               |ORDER BY seqnum ASC
               |LIMIT ?""".stripMargin
          private val Insert =
            """|INSERT INTO mailable(seqnum, sha3_256, mail_from, mail_reply_to, mail_to, mail_subject, template_params, retried)
               |VALUES ( nextval('mailable_seq'), ?, ?, ?, ?, ?, ?, ? )""".stripMargin
          private val DeleteSingle =
            """|DELETE FROM mailable
               |WHERE seqnum = ?""".stripMargin
          def selectForDelivery( conn : Connection, batchSize : Int ) : Set[MailSpec.WithHash] = 
            Using.resource( conn.prepareStatement( this.SelectForDelivery ) ): ps =>
              ps.setInt( 1, batchSize )
              Using.resource( ps.executeQuery() ): rs =>
                toSet(rs)( rs => MailSpec.WithHash( rs.getLong(1), Hash.SHA3_256.withHexBytes( rs.getString(2) ), rs.getString(3), Option(rs.getString(4)), Destination(rs.getString(5)), rs.getString(6), TemplateParams(rs.getString(7)), rs.getInt(8) ) )
          def insert( conn : Connection, hash : Hash.SHA3_256, from : String, replyTo : Option[String], to : Destination, subject : String, templateParams : TemplateParams, retried : Int ) =
            Using.resource( conn.prepareStatement( this.Insert ) ): ps =>
              ps.setString         (1, hash.hex)
              ps.setString         (2, from)
              setStringOptional(ps, 3, Types.VARCHAR, replyTo)
              ps.setString         (4, to.toString())
              ps.setString         (5, subject)
              ps.setString         (6, templateParams.toString())
              ps.setInt            (7, retried)
              ps.executeUpdate()
          def insertBatch( conn : Connection, hash : Hash.SHA3_256, from : String, replyTo : Option[String], tosWithTemplateParams : Set[(Destination,TemplateParams)], subject : String, retried : Int ) =
            Using.resource( conn.prepareStatement( this.Insert ) ): ps =>
              tosWithTemplateParams.foreach: (to, templateParams) =>
                ps.setString         (1, hash.hex)
                ps.setString         (2, from)
                setStringOptional(ps, 3, Types.VARCHAR, replyTo)
                ps.setString         (4, to.toString())
                ps.setString         (5, subject)
                ps.setString         (6, templateParams.toString())
                ps.setInt            (7, retried)
                ps.addBatch()
              ps.executeBatch()
          def deleteSingle( conn : Connection, seqnum : Long ) =
            Using.resource( conn.prepareStatement( this.DeleteSingle ) ): ps =>
              ps.setLong( 1, seqnum )
              ps.executeUpdate()
          object Sequence:
            object MailableSeq extends Creatable:
              protected val Create = "CREATE SEQUENCE mailable_seq AS BIGINT"
      end Table
      object Join:
        object ItemAssignment:
          private val SelectItemContentsForAssignable =
            """|SELECT title, author, article, publication_date, link
               |FROM item
               |INNER JOIN assignment
               |ON item.feed_url = assignment.feed_url AND item.guid = assignment.guid
               |WHERE item.feed_url = ? AND assignment.subscribable_name = ? AND assignment.within_type_id = ?""".stripMargin
          def selectItemContentsForAssignable( conn : Connection, feedUrl : FeedUrl, subscribableName : SubscribableName, withinTypeId : String ) : Set[ItemContent] =
            Using.resource( conn.prepareStatement( SelectItemContentsForAssignable ) ): ps =>
              ps.setString(1, feedUrl.toString())
              ps.setString(2, subscribableName.toString())
              ps.setString(3, withinTypeId)
              Using.resource( ps.executeQuery() ): rs =>
                toSet( rs )( rs => ItemContent( Option( rs.getString(1) ), Option( rs.getString(2) ), Option( rs.getString(3) ), Option( rs.getTimestamp(4) ).map( _.toInstant ), Option( rs.getString(5) ) ) )
        end ItemAssignment
      end Join

