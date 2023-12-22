package com.mchange.feedletter.db

import java.sql.{Connection,Statement,Timestamp,Types}
import java.time.Instant
import scala.util.Using

import com.mchange.feedletter.{ConfigKey,Destination,ExcludedItem,FeedId,FeedInfo,FeedUrl,Guid,InvalidSubscriptionType,ItemContent,SubscribableName,SubscriptionType,TemplateParams}

import com.mchange.cryptoutil.{Hash, given}

import com.mchange.feedletter.*
import zio.http.Status.SeeOther

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
          protected val Create = "CREATE TABLE config( key VARCHAR(64) PRIMARY KEY, value VARCHAR(1024) NOT NULL )"
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

        object Times extends Creatable:
          protected val Create = "CREATE TABLE times( key VARCHAR(64) PRIMARY KEY, value TIMESTAMP NOT NULL )"
          private val Select = "SELECT value FROM times WHERE key = ?"
          private val Upsert =
            """|INSERT INTO times(key, value)
               |VALUES ( ?, ? )
               |ON CONFLICT(key) DO UPDATE
               |SET value = ?""".stripMargin
          def select( conn : Connection, key : TimesKey ) : Option[Instant] =
            Using.resource( conn.prepareStatement( this.Select ) ): ps =>
              ps.setString(1, key.toString())
              Using.resource( ps.executeQuery() ): rs =>
                zeroOrOneResult("select-times-item", rs)( _.getTimestamp(1).toInstant() )
          def upsert( conn : Connection, key : TimesKey, value : Instant ) =
            Using.resource( conn.prepareStatement( this.Upsert ) ): ps =>
              val tvalue = Timestamp.from(value)
              ps.setString   (1, key.toString())
              ps.setTimestamp(2, tvalue )
              ps.setTimestamp(3, tvalue )
              ps.executeUpdate()

        object Feed extends Creatable:
          protected val Create =
            """|CREATE TABLE feed(
               |  id                          INTEGER,
               |  url                         VARCHAR(1024),
               |  min_delay_minutes           INTEGER NOT NULL,
               |  await_stabilization_minutes INTEGER NOT NULL,
               |  max_delay_minutes           INTEGER NOT NULL,
               |  added                       TIMESTAMP NOT NULL,
               |  last_assigned               TIMESTAMP NOT NULL,     -- we'll start at added
               |  PRIMARY KEY(id)
               |)""".stripMargin
          private val Insert =
            """|INSERT INTO feed(id, url, min_delay_minutes, await_stabilization_minutes, max_delay_minutes, added, last_assigned)
               |VALUES( ?, ?, ?, ?, ?, ?, ? )""".stripMargin
          private val SelectAll =
            "SELECT id, url, min_delay_minutes, await_stabilization_minutes, max_delay_minutes, added, last_assigned FROM feed"
          private val SelectUrl =
            """|SELECT url
               |FROM feed
               |WHERE id = ?""".stripMargin
          private val SelectLastAssigned =
            """|SELECT last_assigned
               |FROM feed
               |WHERE id = ?""".stripMargin
          private def UpdateLastAssigned =
            """|UPDATE feed
               |SET last_assigned = ?
               |WHERE id = ?""".stripMargin
          def insert( conn : Connection, newFeedId : FeedId, fi : FeedInfo ) : Int =
            assert( fi.feedId == None, "Cannot insert a FeedInfo with an already assigned id: " + fi )
            insert(conn, newFeedId, fi.feedUrl, fi.minDelayMinutes, fi.awaitStabilizationMinutes, fi.maxDelayMinutes, fi.added, fi.lastAssigned)
          def insert( conn : Connection, feedId : FeedId, feedUrl : FeedUrl, minDelayMinutes : Int, awaitStabilizationMinutes : Int, maxDelayMinutes : Int, added : Instant, lastAssigned : Instant ) : Int =
            Using.resource(conn.prepareStatement(this.Insert)): ps =>
              ps.setInt       (1, feedId.toInt)
              ps.setString    (2, feedUrl.toString())
              ps.setInt       (3, minDelayMinutes )
              ps.setInt       (4, awaitStabilizationMinutes)
              ps.setInt       (5, maxDelayMinutes )
              ps.setTimestamp (6, Timestamp.from(added) )
              ps.setTimestamp (7, Timestamp.from(lastAssigned) )
              ps.executeUpdate()
          def selectAll( conn : Connection ) : Set[FeedInfo] =
            Using.resource( conn.prepareStatement( this.SelectAll ) ): ps =>
              Using.resource( ps.executeQuery() ): rs =>
                toSet(rs)( rs => FeedInfo(Some(FeedId(rs.getInt(1))), FeedUrl(rs.getString(2)), rs.getInt(3), rs.getInt(4), rs.getInt(5), rs.getTimestamp(6).toInstant, rs.getTimestamp(7).toInstant) )
          def selectUrl( conn : Connection, feedId : FeedId ) : Option[FeedUrl] =
            Using.resource(conn.prepareStatement(this.SelectUrl)): ps =>
              ps.setInt(1, feedId.toInt)
              Using.resource( ps.executeQuery() ): rs =>
                zeroOrOneResult("select-feed-url", rs)( rs => FeedUrl(rs.getString(1)) )
          def selectLastAssigned( conn : Connection, feedId : FeedId ) : Option[Instant] =
            Using.resource(conn.prepareStatement(this.SelectLastAssigned)): ps =>
              ps.setInt(1, feedId.toInt)
              Using.resource( ps.executeQuery() ): rs =>
                zeroOrOneResult("select-when-feed-last-assigned", rs)( _.getTimestamp(1).toInstant() )
          def updateLastAssigned( conn : Connection, feedId : FeedId, lastAssigned : Instant ) =
            Using.resource( conn.prepareStatement(this.UpdateLastAssigned) ): ps =>
              ps.setTimestamp(1, Timestamp.from(lastAssigned))
              ps.setInt      (2, feedId.toInt)
              ps.executeUpdate()
          object Sequence:
            object FeedSeq extends Creatable:
              protected val Create = "CREATE SEQUENCE feed_seq AS INTEGER"
              private val SelectNext = "SELECT nextval('feed_seq')"
              def selectNext( conn : Connection ) : FeedId =
                Using.resource( conn.prepareStatement(SelectNext) ): ps =>
                  Using.resource( ps.executeQuery() ): rs =>
                    uniqueResult("select-next-feed-seq", rs)( rs => FeedId( rs.getInt(1) ) )

        object Item extends Creatable:
          object Type:
            object ItemAssignability extends Creatable:
              protected val Create = "CREATE TYPE ItemAssignability AS ENUM ('Unassigned', 'Assigned', 'Cleared', 'Excluded')"
          object Index:
            object ItemAssignability extends Creatable:
              protected val Create = "CREATE INDEX item_assignability ON item(assignability)"
          protected val Create =
            """|CREATE TABLE item(
               |  feed_id          INTEGER,
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
               |  PRIMARY KEY(feed_id, guid),
               |  FOREIGN KEY(feed_id) REFERENCES feed(id)
               |)""".stripMargin
          private val SelectCheck =
            """|SELECT content_hash, first_seen, last_checked, stable_since, assignability
               |FROM item
               |WHERE feed_id = ? AND guid = ?""".stripMargin
          private val SelectExcluded =
            s"""|SELECT feed_id, guid, title, author, publication_date, link
                |FROM item
                |WHERE assignability = '${ItemAssignability.Excluded}'""".stripMargin
          private val Insert =
            """|INSERT INTO item(feed_id, guid, title, author, article, publication_date, link, content_hash, first_seen, last_checked, stable_since, assignability)
               |VALUES( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CAST( ? AS ItemAssignability ) )""".stripMargin
          private val UpdateChanged =
            """|UPDATE item
               |SET title = ?, author = ?, article = ?, publication_date = ?, link = ?, content_hash = ?, last_checked = ?, stable_since = ?, assignability = CAST( ? AS ItemAssignability )
               |WHERE feed_id = ? AND guid = ?""".stripMargin
          private val UpdateStable =
            """|UPDATE item
               |SET last_checked = ?
               |WHERE feed_id = ? AND guid = ?""".stripMargin
          private val UpdateAssignability =
            """|UPDATE item
               |SET assignability = CAST( ? AS ItemAssignability )
               |WHERE feed_id = ? AND guid = ?""".stripMargin
          // See
          //   https://stackoverflow.com/questions/178479/preparedstatement-in-clause-alternatives
          //   https://stackoverflow.com/questions/34627026/in-vs-any-operator-in-postgresql
          private val DeleteDisappearedUnassigned =
            """|DELETE FROM item
               |WHERE assignability = 'Unassigned' AND NOT (guid = ANY( ? ))""".stripMargin
          def deleteDisappearedUnassigned( conn : Connection, current : Set[Guid] ) : Int =
            Using.resource( conn.prepareStatement( DeleteDisappearedUnassigned ) ): ps =>
              val sqlArray = conn.createArrayOf("VARCHAR", current.map(_.toString()).toArray)
              ps.setArray(1, sqlArray)
              ps.executeUpdate()
          def checkStatus( conn : Connection, feedId : FeedId, guid : Guid ) : Option[ItemStatus] =
            Using.resource( conn.prepareStatement( SelectCheck ) ): ps =>
              ps.setInt   (1, feedId.toInt)
              ps.setString(2, guid.toString())
              Using.resource( ps.executeQuery() ): rs =>
                zeroOrOneResult("item-check-select", rs): rs =>
                  ItemStatus( rs.getInt(1), rs.getTimestamp(2).toInstant(), rs.getTimestamp(3).toInstant(), rs.getTimestamp(4).toInstant(), ItemAssignability.valueOf(rs.getString(5)) )
          def selectExcluded( conn : Connection ) : Set[ExcludedItem] =
            Using.resource( conn.prepareStatement(SelectExcluded) ): ps =>
              Using.resource( ps.executeQuery() ): rs =>
                toSet(rs)( rs => ExcludedItem( FeedId( rs.getInt(1) ), rs.getString(2), Option( rs.getString(3) ), Option( rs.getString(4) ), Option( rs.getTimestamp(5) ).map( _.toInstant ), Option( rs.getString(6) ) ) )
          def updateStable( conn : Connection, feedId : FeedId, guid : Guid, lastChecked : Instant ) =
            Using.resource( conn.prepareStatement( this.UpdateStable) ): ps =>
              ps.setTimestamp(1, Timestamp.from(lastChecked))
              ps.setInt      (2, feedId.toInt)
              ps.setString   (3, guid.toString())
              ps.executeUpdate()
          def updateChanged( conn : Connection, feedId : FeedId, guid : Guid, newContent : ItemContent, newStatus : ItemStatus ) =
            Using.resource( conn.prepareStatement( this.UpdateChanged ) ): ps =>
              setStringOptional   (ps,  1, Types.VARCHAR, newContent.title )
              setStringOptional   (ps,  2, Types.VARCHAR, newContent.author )
              setStringOptional   (ps,  3, Types.CLOB, newContent.article )
              setTimestampOptional(ps,  4, newContent.pubDate.map( Timestamp.from ))
              setStringOptional   (ps,  5, Types.VARCHAR, newContent.link)
              ps.setInt               ( 6, newStatus.contentHash)
              ps.setTimestamp         ( 7, Timestamp.from(newStatus.lastChecked))
              ps.setTimestamp         ( 8, Timestamp.from(newStatus.stableSince))
              ps.setString            ( 9, newStatus.assignability.toString())
              ps.setInt               (10, feedId.toInt)
              ps.setString            (11, guid.toString())
              ps.executeUpdate()
          def updateAssignability( conn : Connection, feedId : FeedId, guid : Guid, assignability : ItemAssignability ) =
            Using.resource( conn.prepareStatement( this.UpdateAssignability) ): ps =>
              ps.setString(1, assignability.toString())
              ps.setInt   (2, feedId.toInt)
              ps.setString(3, guid.toString())
              ps.executeUpdate()
          def insertNew( conn : Connection, feedId : FeedId, guid : Guid, itemContent : ItemContent, assignability : ItemAssignability ) : Int =
            Using.resource( conn.prepareStatement( Insert ) ): ps =>
              val now = Instant.now
              ps.setInt              (  1, feedId.toInt )
              ps.setString           (  2, guid.toString() )
              setStringOptional   (ps,  3, Types.VARCHAR, itemContent.title)
              setStringOptional   (ps,  4, Types.VARCHAR, itemContent.author)
              setStringOptional   (ps,  5, Types.CLOB, itemContent.article)
              setTimestampOptional(ps,  6, itemContent.pubDate.map( Timestamp.from ))
              setStringOptional   (ps,  7, Types.VARCHAR, itemContent.link)
              ps.setInt              (  8, itemContent.## )
              ps.setTimestamp        (  9, Timestamp.from( now ) )
              ps.setTimestamp        ( 10, Timestamp.from( now ) )
              ps.setTimestamp        ( 11, Timestamp.from( now ) )
              ps.setString           ( 12, assignability.toString() )
              ps.executeUpdate()
        object Subscribable extends Creatable:
          protected val Create =
            """|CREATE TABLE subscribable(
               |  subscribable_name VARCHAR(64),
               |  feed_id           INTEGER,
               |  subscription_type TEXT,
               |  PRIMARY KEY (subscribable_name),
               |  FOREIGN KEY (feed_id) REFERENCES feed(id)
               |)""".stripMargin
          private val Select = "SELECT subscribable_name, feed_id, subscription_type FROM subscribable"
          private val SelectFeedIdAndType =
            """|SELECT feed_id, subscription_type
               |FROM subscribable
               |WHERE subscribable_name = ?""".stripMargin
          private val SelectType =
            """|SELECT subscription_type
               |FROM subscribable
               |WHERE subscribable_name = ?""".stripMargin
          private val UpdateType =
            """|UPDATE subscribable
               |SET subscription_type = ?
               |WHERE subscribable_name = ?""".stripMargin
          private val Insert = "INSERT INTO subscribable VALUES ( ?, ?, ? )"
          private val SelectSubscriptionTypeNamesByFeedId =
            """|SELECT DISTINCT subscribable_name
               |FROM subscribable
               |WHERE subscribable.feed_id = ?""".stripMargin
          def updateSubscriptionType( conn : Connection, subscribableName : SubscribableName, subscriptionType : SubscriptionType ) =
            Using.resource( conn.prepareStatement( UpdateType ) ): ps =>
              ps.setString(1, subscriptionType.toString())
              ps.setString(2, subscribableName.toString())
              ps.executeUpdate()
          def selectSubscriptionTypeNamesByFeedId( conn : Connection, feedId : FeedId ) : Set[SubscribableName] =
            Using.resource( conn.prepareStatement( this.SelectSubscriptionTypeNamesByFeedId ) ): ps =>
              ps.setInt(1, feedId.toInt )
              Using.resource( ps.executeQuery() ): rs =>
                toSet(rs)( rs => SubscribableName( rs.getString(1) ) )
          def select( conn : Connection ) : Set[(SubscribableName, FeedId, SubscriptionType)] =
            Using.resource( conn.prepareStatement( Select ) ): ps =>
              Using.resource( ps.executeQuery() ): rs =>
                toSet(rs)( rs => ( SubscribableName( rs.getString(1) ), FeedId( rs.getInt(2) ), SubscriptionType.parse( rs.getString(3) ) ) )
          def selectTypeRep( conn : Connection, subscribableName : SubscribableName ) : String =
            Using.resource( conn.prepareStatement( SelectType ) ): ps =>
              ps.setString(1, subscribableName.toString())
              Using.resource( ps.executeQuery() ): rs =>
                uniqueResult("select-subscription-type-rep", rs)( _.getString(1) )
          def selectType( conn : Connection, subscribableName : SubscribableName ) : SubscriptionType =
            SubscriptionType.parse( selectTypeRep(conn, subscribableName) )
          def selectFeedIdAndType( conn : Connection, subscribableName : SubscribableName ) : (FeedId, SubscriptionType) =
            Using.resource( conn.prepareStatement( SelectFeedIdAndType ) ): ps =>
              ps.setString(1, subscribableName.toString())
              Using.resource( ps.executeQuery() ): rs =>
                uniqueResult("select-subscription-type", rs)( rs => ( FeedId( rs.getInt(1) ), SubscriptionType.parse( rs.getString(2) ) ) )
          def insert( conn : Connection, subscribableName : SubscribableName, feedId : FeedId, subscriptionType : SubscriptionType ) =
            Using.resource( conn.prepareStatement( Insert ) ): ps =>
              ps.setString(1, subscribableName.toString())
              ps.setInt   (2, feedId.toInt)
              ps.setString(3, subscriptionType.toString())
              ps.executeUpdate()
        object Assignable extends Creatable:
          protected val Create = // an assignable represents a collection of posts for a single mail
            """|CREATE TABLE assignable(
               |  subscribable_name VARCHAR(64),
               |  within_type_id    VARCHAR(1024),
               |  opened            TIMESTAMP NOT NULL,
               |  completed         TIMESTAMP,
               |  PRIMARY KEY(subscribable_name, within_type_id),
               |  FOREIGN KEY(subscribable_name) REFERENCES subscribable(subscribable_name)
               |)""".stripMargin
          private val SelectIsCompleted =
            """|SELECT completed IS NOT NULL
               |FROM assignable
               |WHERE subscribable_name = ? AND within_type_id = ?""".stripMargin
          private val SelectOpen =
            """|SELECT subscribable_name, within_type_id
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
               |WHERE subscribable_name = ?
               |ORDER BY opened DESC
               |LIMIT 1""".stripMargin
          private val SelectWithinTypeIdLastCompleted =
            """|SELECT within_type_id
               |FROM assignable
               |WHERE completed IS NOT NULL AND subscribable_name = ?
               |ORDER BY completed DESC
               |LIMIT 1""".stripMargin // usually there will always be 1 !
          private val Insert =
            """|INSERT INTO assignable( subscribable_name, within_type_id, opened, completed )
               |VALUES ( ?, ?, ?, ? )""".stripMargin
          private val UpdateCompleted =
            """|UPDATE assignable
               |SET completed = ?
               |WHERE subscribable_name = ? AND within_type_id = ?""".stripMargin
          private val Delete =
            """|DELETE FROM assignable
               |WHERE subscribable_name = ? AND within_type_id = ?""".stripMargin
          def selectOpen( conn : Connection ) : Set[AssignableKey] =
            Using.resource( conn.prepareStatement( this.SelectOpen ) ): ps =>
              Using.resource( ps.executeQuery() ): rs =>
                toSet(rs)( rs => AssignableKey( SubscribableName( rs.getString(1) ), rs.getString(2) ) )
          def selectIsCompleted( conn : Connection, subscribableName : SubscribableName, withinTypeId : String ) : Option[Boolean] =
            Using.resource( conn.prepareStatement( this.SelectIsCompleted ) ): ps =>
              ps.setString(1, subscribableName.toString())
              ps.setString(2, withinTypeId)
              Using.resource(ps.executeQuery()): rs =>
                zeroOrOneResult("assignable-select-completed", rs)( _.getBoolean(1) )
          def selectWithinTypeIdMostRecentOpen( conn : Connection, subscribableName : SubscribableName ) : Option[String] =
            Using.resource( conn.prepareStatement( this.SelectWithinTypeIdMostRecentOpen ) ): ps =>
              ps.setString(1, subscribableName.toString())
              Using.resource(ps.executeQuery()): rs =>
                zeroOrOneResult("assignable-select-most-recent-open-within-type-id", rs)( _.getString(1) )
          def selectWithinTypeIdLastCompleted( conn : Connection, subscribableName : SubscribableName ) : Option[String] =
            Using.resource( conn.prepareStatement( this.SelectWithinTypeIdLastCompleted ) ): ps =>
              ps.setString(1, subscribableName.toString())
              Using.resource(ps.executeQuery()): rs =>
                zeroOrOneResult("assignable-select-last-completed-within-type-id", rs)( _.getString(1) )
          def insert( conn : Connection, subscribableName : SubscribableName, withinTypeId : String, opened : Instant, completed : Option[Instant] ) =
            Using.resource( conn.prepareStatement( this.Insert ) ): ps =>
              ps.setString            (1, subscribableName.toString())
              ps.setString            (2, withinTypeId)
              ps.setTimestamp         (3, Timestamp.from(opened))
              setTimestampOptional(ps, 4, completed.map( Timestamp.from ))
              ps.executeUpdate()
          def updateCompleted( conn : Connection, subscribableName : SubscribableName, withinTypeId : String, completed : Option[Instant] ) =
            Using.resource( conn.prepareStatement( this.UpdateCompleted ) ): ps =>
              completed.fold( ps.setNull(1, Types.TIMESTAMP) )( instant => ps.setTimestamp(1, Timestamp.from(instant)) )
              ps.setString(2, subscribableName.toString())
              ps.setString(3, withinTypeId)
              ps.executeUpdate()
          def delete( conn : Connection, subscribableName : SubscribableName, withinTypeId : String ) =
            Using.resource( conn.prepareStatement( Delete ) ): ps =>
              ps.setString(1, subscribableName.toString())
              ps.setString(2, withinTypeId)
              ps.executeUpdate()
        object Assignment extends Creatable:
          protected val Create = // an assignment represents a membership of a post in a collection
            """|CREATE TABLE assignment(
               |  subscribable_name VARCHAR(64),
               |  within_type_id    VARCHAR(1024),
               |  guid              VARCHAR(1024),
               |  PRIMARY KEY( subscribable_name, within_type_id, guid ),
               |  FOREIGN KEY( subscribable_name, within_type_id ) REFERENCES assignable( subscribable_name, within_type_id )
               |)""".stripMargin
          private val SelectCountWithinAssignable =
            """|SELECT COUNT(*)
               |FROM assignment
               |WHERE subscribable_name = ? AND within_type_id = ?""".stripMargin
          private val Insert =
            """|INSERT INTO assignment( subscribable_name, within_type_id, guid )
               |VALUES ( ?, ?, ? )""".stripMargin
          private val CleanAwayAssignable =
            """|DELETE FROM assignment
               |WHERE subscribable_name = ? AND within_type_id = ?""".stripMargin
          def selectCountWithinAssignable( conn : Connection, subscribableName : SubscribableName, withinTypeId : String ) : Int =
            Using.resource( conn.prepareStatement( this.SelectCountWithinAssignable ) ): ps =>
              ps.setString(1, subscribableName.toString())
              ps.setString(2, withinTypeId)
              Using.resource( ps.executeQuery() ): rs =>
                uniqueResult( "select-count-within-assignable", rs )( _.getInt(1) )
          def insert( conn : Connection, subscribableName : SubscribableName, withinTypeId : String, guid : Guid ) =
            Using.resource( conn.prepareStatement( this.Insert ) ): ps =>
              ps.setString(1, subscribableName.toString())
              ps.setString(2, withinTypeId)
              ps.setString(3, guid.toString())
              ps.executeUpdate()
          def cleanAwayAssignable( conn : Connection, subscribableName : SubscribableName, withinTypeId : String ) =
            Using.resource( conn.prepareStatement( CleanAwayAssignable ) ): ps =>
              ps.setString(1, subscribableName.toString())
              ps.setString(2, withinTypeId)
              ps.executeUpdate()

        object Subscription extends Creatable:
          protected val Create =
            """|CREATE TABLE subscription(
               |  destination       VARCHAR(1024),
               |  subscribable_name VARCHAR(64),
               |  PRIMARY KEY( destination, subscribable_name ),
               |  FOREIGN KEY( subscribable_name ) REFERENCES subscribable( subscribable_name )
               |)""".stripMargin
          private val SelectDestination =
            """|SELECT destination
               |FROM subscription
               |WHERE subscribable_name = ?""".stripMargin
          private val Insert =
            """|INSERT INTO subscription(destination, subscribable_name)
               |VALUES ( ?, ? )""".stripMargin
          def selectDestination( conn : Connection, subscribableName : SubscribableName ) : Set[Destination] =
            Using.resource( conn.prepareStatement( this.SelectDestination ) ): ps =>
              ps.setString(1, subscribableName.toString())
              Using.resource( ps.executeQuery() ): rs =>
                toSet(rs)( rs => Destination( rs.getString(1) ) )
          def insert( conn : Connection, destination : Destination, subscribableName : SubscribableName ) =
            Using.resource( conn.prepareStatement( Insert ) ): ps =>
              ps.setString(1, destination.toString())
              ps.setString(2, subscribableName.toString())
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
          private val DeleteIfUnreferenced =
            """|DELETE FROM mailable_template
               |WHERE sha3_256 = ? AND NOT EXISTS (
               |  SELECT 1 FROM mailable WHERE mailable.sha3_256 = ?
               |)
               |""".stripMargin
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
          def deleteIfUnreferenced( conn : Connection, hash : Hash.SHA3_256 ) =
            val hex = hash.hex
            Using.resource( conn.prepareStatement( DeleteIfUnreferenced ) ): ps =>
              ps.setString( 1, hex )
              ps.setString( 2, hex )
              ps.executeUpdate()
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
        object ItemSubscribable:
          private val SelectFeedIdUrlForSubscribableName =
            """|SELECT id, url
               |FROM feed
               |INNER JOIN subscribable
               |ON feed.id = subscribable.feed_id
               |WHERE subscribable.subscribable_name = ?""".stripMargin
          def selectFeedIdUrlForSubscribableName( conn : Connection, subscribableName : SubscribableName ) : ( FeedId, FeedUrl ) =
            Using.resource( conn.prepareStatement( SelectFeedIdUrlForSubscribableName ) ): ps =>
              ps.setString(1, subscribableName.toString())
              Using.resource( ps.executeQuery() ): rs =>
                uniqueResult("select-feed-id-url-for-subname", rs): rs =>
                  ( FeedId( rs.getInt(1) ), FeedUrl( rs.getString(2) ) )
          private val SelectFeedUrlSubscriptionTypeForSubscribableName =
            """|SELECT url, subscription_type
               |FROM feed
               |INNER JOIN subscribable
               |ON feed.id = subscribable.feed_id
               |WHERE subscribable.subscribable_name = ?""".stripMargin
          def selectFeedUrlSubscriptionTypeForSubscribableName( conn : Connection, subscribableName : SubscribableName ) : ( FeedUrl, SubscriptionType ) =
            Using.resource( conn.prepareStatement( SelectFeedUrlSubscriptionTypeForSubscribableName ) ): ps =>
              ps.setString(1, subscribableName.toString())
              Using.resource( ps.executeQuery() ): rs =>
                uniqueResult("select-feed-url-subscription-type-for-subname", rs): rs =>
                  ( FeedUrl( rs.getString(1) ), SubscriptionType.parse( rs.getString(2) ) )
        end ItemSubscribable
        object ItemAssignment:
          private val SelectItemContentsForAssignable =
            """|SELECT title, author, article, publication_date, link
               |FROM item
               |INNER JOIN assignment
               |ON item.guid = assignment.guid
               |WHERE assignment.subscribable_name = ? AND assignment.within_type_id = ?""".stripMargin
          def selectItemContentsForAssignable( conn : Connection, subscribableName : SubscribableName, withinTypeId : String ) : Set[ItemContent] =
            Using.resource( conn.prepareStatement( SelectItemContentsForAssignable ) ): ps =>
              ps.setString(1, subscribableName.toString())
              ps.setString(2, withinTypeId)
              Using.resource( ps.executeQuery() ): rs =>
                toSet( rs )( rs => ItemContent( Option( rs.getString(1) ), Option( rs.getString(2) ), Option( rs.getString(3) ), Option( rs.getTimestamp(4) ).map( _.toInstant ), Option( rs.getString(5) ) ) )
        end ItemAssignment
        object ItemAssignableAssignment:
          private val SelectLiveAssignedGuids =
             """|SELECT guid
                |FROM assignable
                |INNER JOIN assignment
                |ON assignable.subscribable_name = assignment.subscribable_name AND assignable.within_type_id = assignment.within_type_id""".stripMargin
          private val ClearOldCache =
             s"""|UPDATE item
                 |SET title = NULL, author = NULL, article = NULL, publication_date = NULL, link = NULL, content_hash = ${ItemContent.EmptyHashCode}, assignability = 'Cleared'
                 |WHERE assignability = 'Assigned' AND NOT item.guid IN ( ${SelectLiveAssignedGuids} )""".stripMargin
          def clearOldCache( conn : Connection ) =
            Using.resource( conn.prepareStatement( ClearOldCache ) )( _.executeUpdate() )
        end ItemAssignableAssignment
      end Join

