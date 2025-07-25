package com.mchange.feedletter.db

import java.sql.{Connection,Statement,Timestamp,Types}
import java.time.Instant
import scala.util.Using

import com.mchange.cryptoutil.{Hash, given}

import com.mchange.sc.sqlutil.*
import com.mchange.sc.sqlutil.migrate.*

import com.mchange.feedletter.*
import com.mchange.feedletter.Destination.Key

import LoggingApi.*

object PgSchema extends SelfLogging:
  object Unversioned:
    object Table:
      object Metadata extends Creatable: // Creatable is now defined in sqlutil
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

  object V0 extends Schema: // contains unversioned schema only
    override val Version = 0
  object V1 extends Schema:
    override val Version = 1
    object Table:
      // careful updating this one, as new versions of the app need to be able to set --dump-db-dir even against old schemas
      // see special case in CommandConfig.DbDump
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

      object Flags extends Creatable:
        protected val Create = "CREATE TABLE flags( flag VARCHAR(64) PRIMARY KEY )"
        private val Select = "SELECT flag FROM flags WHERE flag = ?"
        private val Upsert =
          """|INSERT INTO flags(flag)
             |VALUES ( ? )
             |ON CONFLICT(flag) DO NOTHING""".stripMargin
        private val Delete = "DELETE FROM flags WHERE flag = ?"
        def isSet( conn : Connection, flag : Flag ) : Boolean =
          Using.resource( conn.prepareStatement( this.Select ) ): ps =>
            ps.setString(1, flag.toString())
            Using.resource( ps.executeQuery() )( _.next() )
        def set( conn : Connection, flag : Flag ) =
          Using.resource( conn.prepareStatement( this.Upsert ) ): ps =>
            ps.setString(1, flag.toString())
            ps.executeUpdate()
        def unset( conn : Connection, flag : Flag ) =
          Using.resource( conn.prepareStatement( this.Delete ) ): ps =>
            ps.setString(1, flag.toString())
            ps.executeUpdate()

      object Feed extends Creatable:
        protected val Create =
          """|CREATE TABLE feed(
             |  id                          INTEGER,
             |  url                         VARCHAR(1024),
             |  min_delay_minutes           INTEGER NOT NULL,
             |  await_stabilization_minutes INTEGER NOT NULL,
             |  max_delay_minutes           INTEGER NOT NULL,
             |  assign_every_minutes        INTEGER NOT NULL,
             |  added                       TIMESTAMP NOT NULL,
             |  last_assigned               TIMESTAMP NOT NULL,     -- we'll start at added
             |  PRIMARY KEY(id)
             |)""".stripMargin
        private val Insert =
          """|INSERT INTO feed(id, url, min_delay_minutes, await_stabilization_minutes, max_delay_minutes, assign_every_minutes, added, last_assigned)
             |VALUES( ?, ?, ?, ?, ?, ?, ?, ? )""".stripMargin
        private val SelectAll =
          "SELECT id, url, min_delay_minutes, await_stabilization_minutes, max_delay_minutes, assign_every_minutes, added, last_assigned FROM feed"
        private val SelectById = SelectAll + " WHERE id = ?"  
        private val SelectUrl =
          """|SELECT url
             |FROM feed
             |WHERE id = ?""".stripMargin
        private val SelectLastAssigned =
          """|SELECT last_assigned
             |FROM feed
             |WHERE id = ?""".stripMargin
        private val UpdateLastAssigned =
          """|UPDATE feed
             |SET last_assigned = ?
             |WHERE id = ?""".stripMargin
        private val UpdateFeedTimings =
          """|UPDATE feed
             |SET min_delay_minutes = ?, await_stabilization_minutes = ?, max_delay_minutes = ?, assign_every_minutes = ?
             |WHERE id = ?""".stripMargin
        private val Delete =
          """|DELETE FROM feed
             |WHERE id = ?""".stripMargin
        def delete( conn : Connection, feedId : FeedId ) =
          Using.resource( conn.prepareStatement(Delete) ): ps =>
            ps.setInt(1, feedId.toInt)
            ps.executeUpdate()
        def updateFeedTimings( conn : Connection, feedId : FeedId, minDelayMinutes : Int, awaitStabilizationMinutes : Int, maxDelayMinutes : Int, assignEveryMinutes : Int ) =
          Using.resource( conn.prepareStatement(UpdateFeedTimings) ): ps =>
            ps.setInt( 1, minDelayMinutes )
            ps.setInt( 2, awaitStabilizationMinutes )
            ps.setInt( 3, maxDelayMinutes )
            ps.setInt( 4, assignEveryMinutes )
            ps.setInt( 5, feedId.toInt )
            ps.executeUpdate()
        def insert( conn : Connection, newFeedId : FeedId, nf : NascentFeed ) : Int =
          insert(conn, newFeedId, nf.feedUrl, nf.minDelayMinutes, nf.awaitStabilizationMinutes, nf.maxDelayMinutes, nf.assignEveryMinutes, nf.added, nf.lastAssigned)
        def insert( conn : Connection, feedId : FeedId, feedUrl : FeedUrl, minDelayMinutes : Int, awaitStabilizationMinutes : Int, maxDelayMinutes : Int, assignEveryMinutes : Int, added : Instant, lastAssigned : Instant ) : Int =
          Using.resource(conn.prepareStatement(this.Insert)): ps =>
            ps.setInt       (1, feedId.toInt)
            ps.setString    (2, feedUrl.str)
            ps.setInt       (3, minDelayMinutes )
            ps.setInt       (4, awaitStabilizationMinutes)
            ps.setInt       (5, maxDelayMinutes )
            ps.setInt       (6, assignEveryMinutes )
            ps.setTimestamp (7, Timestamp.from(added) )
            ps.setTimestamp (8, Timestamp.from(lastAssigned) )
            ps.executeUpdate()
        def selectAll( conn : Connection ) : Set[FeedInfo] =
          Using.resource( conn.prepareStatement( this.SelectAll ) ): ps =>
            Using.resource( ps.executeQuery() ): rs =>
              toSet(rs)( rs => FeedInfo(FeedId(rs.getInt(1)), FeedUrl(rs.getString(2)), rs.getInt(3), rs.getInt(4), rs.getInt(5), rs.getInt(6), rs.getTimestamp(7).toInstant, rs.getTimestamp(8).toInstant) )
        def selectById( conn : Connection, id : FeedId ) : FeedInfo =
          Using.resource( conn.prepareStatement( this.SelectById ) ): ps =>
            ps.setInt(1, id.toInt)
            Using.resource( ps.executeQuery() ): rs =>
              uniqueResult("feed-select-by-id", rs): rs =>
                FeedInfo(FeedId(rs.getInt(1)), FeedUrl(rs.getString(2)), rs.getInt(3), rs.getInt(4), rs.getInt(5), rs.getInt(6), rs.getTimestamp(7).toInstant, rs.getTimestamp(8).toInstant)
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
             |  single_item_rss  TEXT,
             |  content_hash     INTEGER, -- ItemContent.contentHash
             |  link             VARCHAR(1024),
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
          s"""|SELECT feed_id, guid, link
              |FROM item
              |WHERE assignability = '${ItemAssignability.Excluded}'""".stripMargin
        private val Insert =
          """|INSERT INTO item(feed_id, guid, single_item_rss, content_hash, link, first_seen, last_checked, stable_since, assignability)
             |VALUES( ?, ?, ?, ?, ?, ?, ?, ?, CAST( ? AS ItemAssignability ) )""".stripMargin
        private val UpdateChanged =
          """|UPDATE item
             |SET single_item_rss = ?, content_hash = ?, link = ?, last_checked = ?, stable_since = ?, assignability = CAST( ? AS ItemAssignability )
             |WHERE feed_id = ? AND guid = ?""".stripMargin
        private val UpdateStable =
          """|UPDATE item
             |SET last_checked = ?
             |WHERE feed_id = ? AND guid = ?""".stripMargin
        private val UpdateLastCheckedAssignability =
          """|UPDATE item
             |SET last_checked = ?, assignability = CAST( ? AS ItemAssignability )
             |WHERE feed_id = ? AND guid = ?""".stripMargin
        // See
        //   https://stackoverflow.com/questions/178479/preparedstatement-in-clause-alternatives
        //   https://stackoverflow.com/questions/34627026/in-vs-any-operator-in-postgresql
        private val DeleteDisappearedUnassignedForFeed =
          """|DELETE FROM item
             |WHERE assignability = 'Unassigned' AND feed_id = ? AND NOT (guid = ANY( ? ))""".stripMargin
        private val DeleteByFeed =
          """|DELETE FROM item
             |WHERE feed_id = ?""".stripMargin
        def deleteByFeed( conn : Connection, feedId : FeedId ) =
          Using.resource( conn.prepareStatement( DeleteByFeed ) ): ps =>
            ps.setInt   (1, feedId.toInt)
            ps.executeUpdate()
        def deleteDisappearedUnassignedForFeed( conn : Connection, feedId : FeedId, current : Set[Guid] ) : Int =
          Using.resource( conn.prepareStatement( DeleteDisappearedUnassignedForFeed ) ): ps =>
            val sqlArray = conn.createArrayOf("VARCHAR", current.map(_.str).toArray)
            ps.setInt(1, feedId.toInt)
            ps.setArray (2, sqlArray)
            ps.executeUpdate()
        def checkStatus( conn : Connection, feedId : FeedId, guid : Guid ) : Option[ItemStatus] =
          Using.resource( conn.prepareStatement( SelectCheck ) ): ps =>
            ps.setInt   (1, feedId.toInt)
            ps.setString(2, guid.str)
            Using.resource( ps.executeQuery() ): rs =>
              zeroOrOneResult("item-check-select", rs): rs =>
                ItemStatus( rs.getInt(1), rs.getTimestamp(2).toInstant(), rs.getTimestamp(3).toInstant(), rs.getTimestamp(4).toInstant(), ItemAssignability.valueOf(rs.getString(5)) )
        def selectExcluded( conn : Connection ) : Set[ExcludedItem] =
          Using.resource( conn.prepareStatement(SelectExcluded) ): ps =>
            Using.resource( ps.executeQuery() ): rs =>
              toSet(rs)( rs => ExcludedItem( FeedId(rs.getInt(1)), Guid(rs.getString(2)), Option(rs.getString(3)) ) )
        def updateStable( conn : Connection, feedId : FeedId, guid : Guid, lastChecked : Instant ) =
          Using.resource( conn.prepareStatement( this.UpdateStable) ): ps =>
            ps.setTimestamp(1, Timestamp.from(lastChecked))
            ps.setInt      (2, feedId.toInt)
            ps.setString   (3, guid.str)
            ps.executeUpdate()
        def updateChanged( conn : Connection, feedId : FeedId, guid : Guid, newContent : ItemContent, newStatus : ItemStatus ) =
          Using.resource( conn.prepareStatement( this.UpdateChanged ) ): ps =>
            ps.setString            ( 1, newContent.rssElemBeforeOverrides.toString )
            ps.setInt               ( 2, newStatus.contentHash)
            setStringOptional   (ps,  3, Types.VARCHAR, newContent.link)
            ps.setTimestamp         ( 4, Timestamp.from(newStatus.lastChecked))
            ps.setTimestamp         ( 5, Timestamp.from(newStatus.stableSince))
            ps.setString            ( 6, newStatus.assignability.toString())
            ps.setInt               ( 7, feedId.toInt)
            ps.setString            ( 8, guid.str)
            ps.executeUpdate()
        def updateLastCheckedAssignability( conn : Connection, feedId : FeedId, guid : Guid, lastChecked : Instant, assignability : ItemAssignability ) =
          Using.resource( conn.prepareStatement( this.UpdateLastCheckedAssignability) ): ps =>
            ps.setTimestamp(1, Timestamp.from(lastChecked))
            ps.setString   (2, assignability.toString())
            ps.setInt      (3, feedId.toInt)
            ps.setString   (4, guid.str)
            ps.executeUpdate()
        def insertNew( conn : Connection, feedId : FeedId, guid : Guid, itemContent : Option[ItemContent], assignability : ItemAssignability ) : Int =
          Using.resource( conn.prepareStatement( Insert ) ): ps =>
            val now = Instant.now
            ps.setInt              (  1, feedId.toInt )
            ps.setString           (  2, guid.str )
            itemContent.fold(ps.setNull(3, Types.CLOB))   (ic => ps.setString(3, ic.rssElemBeforeOverrides.toString() ))
            itemContent.fold(ps.setNull(4, Types.INTEGER))(ic => ps.setInt(4, ic.contentHash ))
            itemContent.fold(ps.setNull(5, Types.VARCHAR))(ic => setStringOptional(ps, 5, Types.VARCHAR, ic.link))
            ps.setTimestamp        (  6, Timestamp.from( now ) )
            ps.setTimestamp        (  7, Timestamp.from( now ) )
            ps.setTimestamp        (  8, Timestamp.from( now ) )
            ps.setString           (  9, assignability.toString() )
            ps.executeUpdate()
      object Subscribable extends Creatable:
        protected val Create =
          """|CREATE TABLE subscribable(
             |  subscribable_name         VARCHAR(64),
             |  feed_id                   INTEGER       NOT NULL,
             |  subscription_manager_json JSONB         NOT NULL,
             |  last_completed_wti        VARCHAR(1024),
             |  PRIMARY KEY (subscribable_name),
             |  FOREIGN KEY (feed_id) REFERENCES feed(id)
             |)""".stripMargin
        private val Select = "SELECT subscribable_name, feed_id, subscription_manager_json, last_completed_wti FROM subscribable"
        private val Delete =
          """|DELETE FROM subscribable
             |WHERE subscribable_name = ?""".stripMargin
        private val SelectFeedIdAndManager =
          """|SELECT feed_id, subscription_manager_json
             |FROM subscribable
             |WHERE subscribable_name = ?""".stripMargin
        private val SelectManager =
          """|SELECT subscription_manager_json
             |FROM subscribable
             |WHERE subscribable_name = ?""".stripMargin
        private val SelectLastCompletedWti =
          """|SELECT last_completed_wti
             |FROM subscribable
             |WHERE subscribable_name = ?""".stripMargin
        private val SelectByFeed =
          """|SELECT subscribable_name
             |FROM subscribable
             |WHERE feed_id = ?""".stripMargin
        private val UpdateManagerJson =
          """|UPDATE subscribable
             |SET subscription_manager_json = CAST( ? AS JSONB )
             |WHERE subscribable_name = ?""".stripMargin
        private val Insert = "INSERT INTO subscribable VALUES ( ?, ?, CAST( ? AS JSONB ), ? )"
        private val SelectSubscribableNamesByFeedId =
          """|SELECT DISTINCT subscribable_name
             |FROM subscribable
             |WHERE subscribable.feed_id = ?""".stripMargin
        private val UpdateLastCompletedWti =
          """|UPDATE subscribable
             |SET last_completed_wti = ?
             |WHERE subscribable_name = ?""".stripMargin
        def selectByFeed( conn : Connection, feedId : FeedId ) : Set[SubscribableName] =
          Using.resource( conn.prepareStatement(SelectByFeed) ): ps =>
            ps.setInt(1, feedId.toInt)
            Using.resource( ps.executeQuery() ): rs =>
              toSet(rs)( rs => SubscribableName( rs.getString(1) ) )
        def delete( conn : Connection, subscribableName : SubscribableName ) =
          Using.resource( conn.prepareStatement( Delete ) ): ps =>
            ps.setString(1, subscribableName.str)
            ps.executeUpdate()
        def updateLastCompletedWti( conn : Connection, subscribableName : SubscribableName, withinTypeId : String ) =
          Using.resource( conn.prepareStatement( UpdateLastCompletedWti ) ): ps =>
            ps.setString(1, withinTypeId)
            ps.setString(2, subscribableName.str)
            ps.executeUpdate()
        def updateSubscriptionManagerJson( conn : Connection, subscribableName : SubscribableName, subscriptionManager : SubscriptionManager ) =
          Using.resource( conn.prepareStatement( UpdateManagerJson ) ): ps =>
            ps.setString(1, subscriptionManager.json.str)
            ps.setString(2, subscribableName.str)
            ps.executeUpdate()
        def selectSubscribableNamesByFeedId( conn : Connection, feedId : FeedId ) : Set[SubscribableName] =
          Using.resource( conn.prepareStatement( this.SelectSubscribableNamesByFeedId ) ): ps =>
            ps.setInt(1, feedId.toInt )
            Using.resource( ps.executeQuery() ): rs =>
              toSet(rs)( rs => SubscribableName( rs.getString(1) ) )
        def select( conn : Connection ) : Set[(SubscribableName, FeedId, SubscriptionManager, Option[String])] =
          Using.resource( conn.prepareStatement( Select ) ): ps =>
            Using.resource( ps.executeQuery() ): rs =>
              toSet(rs)( rs => ( SubscribableName( rs.getString(1) ), FeedId( rs.getInt(2) ), SubscriptionManager.materialize(SubscriptionManager.Json(rs.getString(3)) ), Option(rs.getString(4)) ) )
        def selectLastCompletedWti( conn : Connection, subscribableName : SubscribableName ) : Option[String] =
          Using.resource( conn.prepareStatement( SelectLastCompletedWti ) ): ps =>
            ps.setString(1, subscribableName.str)
            Using.resource( ps.executeQuery() ): rs =>
              uniqueResult("select-last-completed-wti", rs)( rs => Option(rs.getString(1)) )
        def selectUninterpretedManagerJson( conn : Connection, subscribableName : SubscribableName ) : String =
          Using.resource( conn.prepareStatement( SelectManager ) ): ps =>
            ps.setString(1, subscribableName.str)
            Using.resource( ps.executeQuery() ): rs =>
              uniqueResult("select-uninterpreted-manager-json", rs)( rs => rs.getString(1) )
        def selectManager( conn : Connection, subscribableName : SubscribableName ) : SubscriptionManager =
          val json = SubscriptionManager.Json( selectUninterpretedManagerJson( conn, subscribableName ) )
          SubscriptionManager.materialize( json )
        def selectFeedIdAndManager( conn : Connection, subscribableName : SubscribableName ) : (FeedId, SubscriptionManager) =
          Using.resource( conn.prepareStatement( SelectFeedIdAndManager ) ): ps =>
            ps.setString(1, subscribableName.str)
            Using.resource( ps.executeQuery() ): rs =>
              uniqueResult("select-feed-id-and-subscription-manager", rs)( rs => ( FeedId( rs.getInt(1) ), SubscriptionManager.materialize(SubscriptionManager.Json(rs.getString(2)) ) ) )
        def insert( conn : Connection, subscribableName : SubscribableName, feedId : FeedId, subscriptionManager : SubscriptionManager, lastCompletedWti : Option[String] ) =
          Using.resource( conn.prepareStatement( Insert ) ): ps =>
            ps.setString(1, subscribableName.str)
            ps.setInt   (2, feedId.toInt)
            ps.setString(3, subscriptionManager.json.str)
            setStringOptional(ps, 4, Types.VARCHAR, lastCompletedWti )
            ps.executeUpdate()
      object Assignable extends Creatable:
        protected val Create = // an assignable represents a collection of posts for a single mail
          """|CREATE TABLE assignable(
             |  subscribable_name VARCHAR(64),
             |  within_type_id    VARCHAR(1024),
             |  opened            TIMESTAMP NOT NULL,
             |  PRIMARY KEY(subscribable_name, within_type_id),
             |  FOREIGN KEY(subscribable_name) REFERENCES subscribable(subscribable_name)
             |)""".stripMargin
        private val SelectAllKeys ="SELECT subscribable_name, within_type_id FROM assignable"
        // should I make indexes for this? should I try some more clever/efficient form of query?
        // see
        //   https://stackoverflow.com/questions/3800551/select-first-row-in-each-group-by-group/7630564#7630564
        //   https://stackoverflow.com/questions/1684244/efficient-latest-record-query-with-postgresql
        //   https://www.timescale.com/blog/select-the-most-recent-record-of-many-items-with-postgresql/
        private val SelectWithinTypeIdMostRecentOpened =
          """|SELECT within_type_id
             |FROM assignable
             |WHERE subscribable_name = ?
             |ORDER BY opened DESC
             |LIMIT 1""".stripMargin
        private val SelectOpened =
          """|SELECT opened
             |FROM assignable
             |WHERE subscribable_name = ? AND within_type_id = ?""".stripMargin
        private val Insert =
          """|INSERT INTO assignable( subscribable_name, within_type_id, opened )
             |VALUES ( ?, ?, ? )""".stripMargin
        private val Delete =
          """|DELETE FROM assignable
             |WHERE subscribable_name = ? AND within_type_id = ?""".stripMargin
        private val DeleteAllForSubscribable =
          """|DELETE FROM assignable
             |WHERE subscribable_name = ?""".stripMargin
        def selectOpened( conn : Connection, subscribableName : SubscribableName, withinTypeId : String ) : Option[Instant] =
          Using.resource( conn.prepareStatement( SelectOpened ) ): ps =>
            ps.setString(1, subscribableName.str)
            ps.setString(2, withinTypeId)
            Using.resource( ps.executeQuery() ): rs =>
              zeroOrOneResult("select-open-assignable", rs)( _.getTimestamp(1).toInstant() )
        def selectAllKeys( conn : Connection ) : Set[AssignableKey] =
          Using.resource( conn.prepareStatement( SelectAllKeys ) ): ps =>
            Using.resource( ps.executeQuery() ): rs =>
              toSet(rs)( rs => AssignableKey( SubscribableName( rs.getString(1) ), rs.getString(2) ) )
        def selectWithinTypeIdMostRecentOpened( conn : Connection, subscribableName : SubscribableName ) : Option[String] =
          Using.resource( conn.prepareStatement( this.SelectWithinTypeIdMostRecentOpened ) ): ps =>
            ps.setString(1, subscribableName.str)
            Using.resource(ps.executeQuery()): rs =>
              zeroOrOneResult("assignable-select-most-recent-opened-within-type-id", rs)( _.getString(1) )
        def insert( conn : Connection, subscribableName : SubscribableName, withinTypeId : String, opened : Instant ) =
          Using.resource( conn.prepareStatement( this.Insert ) ): ps =>
            ps.setString   (1, subscribableName.str)
            ps.setString   (2, withinTypeId)
            ps.setTimestamp(3, Timestamp.from(opened))
            ps.executeUpdate()
        def delete( conn : Connection, subscribableName : SubscribableName, withinTypeId : String ) =
          Using.resource( conn.prepareStatement( Delete ) ): ps =>
            ps.setString(1, subscribableName.str)
            ps.setString(2, withinTypeId)
            ps.executeUpdate()
        def deleteAllForSubscribable( conn : Connection, subscribableName : SubscribableName ) =
          Using.resource( conn.prepareStatement( DeleteAllForSubscribable ) ): ps =>
            ps.setString(1, subscribableName.str)
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
        private val CleanAwayAllAssignablesForSubscribable =
          """|DELETE FROM assignment
             |WHERE subscribable_name = ?""".stripMargin
        def selectCountWithinAssignable( conn : Connection, subscribableName : SubscribableName, withinTypeId : String ) : Int =
          Using.resource( conn.prepareStatement( this.SelectCountWithinAssignable ) ): ps =>
            ps.setString(1, subscribableName.str)
            ps.setString(2, withinTypeId)
            Using.resource( ps.executeQuery() ): rs =>
              uniqueResult( "select-count-within-assignable", rs )( _.getInt(1) )
        def insert( conn : Connection, subscribableName : SubscribableName, withinTypeId : String, guid : Guid ) =
          Using.resource( conn.prepareStatement( this.Insert ) ): ps =>
            ps.setString(1, subscribableName.str)
            ps.setString(2, withinTypeId)
            ps.setString(3, guid.str)
            ps.executeUpdate()
        def cleanAwayAssignable( conn : Connection, subscribableName : SubscribableName, withinTypeId : String ) =
          Using.resource( conn.prepareStatement( CleanAwayAssignable ) ): ps =>
            ps.setString(1, subscribableName.str)
            ps.setString(2, withinTypeId)
            ps.executeUpdate()
        def cleanAwayAssignableAllAssignablesForSubscribable( conn : Connection, subscribableName : SubscribableName ) =
          Using.resource( conn.prepareStatement( CleanAwayAllAssignablesForSubscribable ) ): ps =>
            ps.setString(1, subscribableName.str)
            ps.executeUpdate()

      object Subscription extends Creatable:
        protected val Create =
          """|CREATE TABLE subscription(
             |  subscription_id    BIGINT,
             |  destination_json   JSONB         NOT NULL,
             |  destination_unique VARCHAR(1024) NOT NULL,
             |  subscribable_name  VARCHAR(64)   NOT NULL,
             |  confirmed          BOOLEAN       NOT NULL,
             |  added              TIMESTAMP     NOT NULL,
             |  PRIMARY KEY( subscription_id ),
             |  FOREIGN KEY( subscribable_name ) REFERENCES subscribable( subscribable_name )
             |)""".stripMargin
        private val SelectConfirmedIdentifiedDestinationsForSubscribable =
          """|SELECT subscription_id, destination_json
             |FROM subscription
             |WHERE subscribable_name = ? AND confirmed = TRUE""".stripMargin
        private val SelectForSubscribable =
          """|SELECT subscription_id, destination_json, confirmed, added
             |FROM subscription
             |WHERE subscribable_name = ?""".stripMargin
        private val DeleteAllForSubscribable =
          """|DELETE FROM subscription
             |WHERE subscribable_name = ?""".stripMargin
        private val Insert =
          """|INSERT INTO subscription(subscription_id, destination_json, destination_unique, subscribable_name, confirmed, added)
             |VALUES ( ?, CAST( ? AS JSONB ), ?, ?, ?, ? )""".stripMargin
        private val UpdateConfirmed =
          """|UPDATE subscription
             |SET confirmed = ?
             |WHERE subscription_id = ?""".stripMargin
        private val Delete =
          """|DELETE FROM subscription
             |WHERE subscription_id = ?""".stripMargin
        private val ExpireUnconfirmedAddedBefore =
          """|DELETE FROM subscription
             |WHERE confirmed = FALSE AND added < ?""".stripMargin
        private val SubscribersExistForSubscribable = "SELECT EXISTS(SELECT 1 FROM subscription WHERE subscribable_name = ?)"
        private val DestinationAlreadySubscribed    = "SELECT EXISTS(SELECT 1 FROM subscription WHERE destination_unique = ? AND subscribable_name = ?)"
        def destinationAlreadySubscribed( conn : Connection, destination : Destination, subscribableName : SubscribableName ) : Boolean =
          Using.resource( conn.prepareStatement(DestinationAlreadySubscribed) ): ps =>
            ps.setString(1, destination.unique)
            ps.setString(2, subscribableName.str)
            Using.resource( ps.executeQuery() ): rs =>
              uniqueResult("destination-already-subscribed", rs)( _.getBoolean(1) )
        def subscribersExist( conn : Connection, subscribableName : SubscribableName ) : Boolean =
          Using.resource( conn.prepareStatement(SubscribersExistForSubscribable) ): ps =>
            ps.setString(1, subscribableName.str)
            Using.resource( ps.executeQuery() ): rs =>
              uniqueResult("subscribers-exist-for-subscribable", rs)( _.getBoolean(1) )
        def deleteAllForSubscribable( conn : Connection, subscribableName : SubscribableName ) =
          Using.resource( conn.prepareStatement( DeleteAllForSubscribable ) ): ps =>
            ps.setString(1, subscribableName.str )
            ps.executeUpdate()
        def selectForSubscribable( conn : Connection, subscribableName : SubscribableName ) : Set[( SubscriptionId, Destination, Boolean, Instant )] =
          Using.resource( conn.prepareStatement( SelectForSubscribable ) ): ps =>
            ps.setString(1, subscribableName.str )
            Using.resource( ps.executeQuery() ): rs =>
              toSet(rs)( rs => ( SubscriptionId(rs.getLong(1)), Destination.materialize( Destination.Json( rs.getString(2) ) ), rs.getBoolean(3), rs.getTimestamp(4).toInstant ) )
        def expireUnconfirmedAddedBefore( conn : Connection, before : Instant ) : Int =
          Using.resource( conn.prepareStatement( ExpireUnconfirmedAddedBefore ) ): ps =>
            ps.setTimestamp( 1, Timestamp.from(before) )
            ps.executeUpdate()
        def delete( conn : Connection, subscriptionId : SubscriptionId ) =
          Using.resource( conn.prepareStatement( Delete ) ): ps =>
            ps.setLong(1, subscriptionId.toLong)
            ps.executeUpdate()
        def selectConfirmedIdentifiedDestinationsForSubscribable( conn : Connection, subscribableName : SubscribableName ) : Set[IdentifiedDestination[Destination]] =
          Using.resource( conn.prepareStatement( SelectConfirmedIdentifiedDestinationsForSubscribable ) ): ps =>
            ps.setString(1, subscribableName.str)
            Using.resource( ps.executeQuery() ): rs =>
              toSet(rs)( rs => IdentifiedDestination( SubscriptionId( rs.getLong(1) ), Destination.materialize( Destination.Json( rs.getString(2) ) ) ) )
        def insert( conn : Connection, subscriptionId : SubscriptionId, destination : Destination, subscribableName : SubscribableName, confirmed : Boolean, now : Instant ) =
          Using.resource( conn.prepareStatement( Insert ) ): ps =>
            ps.setLong     (1, subscriptionId.toLong)
            ps.setString   (2, destination.json.str)
            ps.setString   (3, destination.unique)
            ps.setString   (4, subscribableName.str)
            ps.setBoolean  (5, confirmed)
            ps.setTimestamp(6, Timestamp.from(now))
            ps.executeUpdate()
        def updateConfirmed( conn : Connection, subscriptionId : SubscriptionId, confirmed : Boolean ) =
          Using.resource( conn.prepareStatement( UpdateConfirmed ) ): ps =>
            ps.setBoolean(1, confirmed)
            ps.setLong   (2, subscriptionId.toLong)
            ps.executeUpdate()
        object Sequence:
          object SubscriptionSeq extends Creatable:
            protected val Create = "CREATE SEQUENCE subscription_seq AS BIGINT"
            private val SelectNext = "SELECT nextval('subscription_seq')"
            def selectNext( conn : Connection ) : SubscriptionId =
              Using.resource( conn.prepareStatement(SelectNext) ): ps =>
                Using.resource( ps.executeQuery() ): rs =>
                  uniqueResult("select-next-feed-seq", rs)( rs => SubscriptionId( rs.getLong(1) ) )
        object Index:
          object SubscriptionIdConfirmed extends Creatable:
            protected val Create = "CREATE INDEX subscription_id_confirmed ON subscription(subscription_id, confirmed)"
          object DestinationUniqueSubscribableName extends Creatable:  
            protected val Create = "CREATE UNIQUE INDEX destination_unique_subscribable_name ON subscription(destination_unique, subscribable_name)"

      // publication-related tables should be decoupled from, unrelated to the
      // tables above. logically, we should be listening for "completion" above
      // as a kind of event, which would trigger SubscriptionManager route potentially
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
        private val MailQueued =
          """SELECT EXISTS (SELECT seqnum FROM mailable LIMIT 1)"""
        def mailQueued( conn : Connection ) : Boolean =
          Using.resource( conn.prepareStatement(this.MailQueued) ): ps =>
            Using.resource( ps.executeQuery() ): rs =>
              uniqueResult[Boolean]("check for existence of queued mail", rs)( rs => rs.getBoolean(1) )
        def selectForDelivery( conn : Connection, batchSize : Int ) : Set[MailSpec.WithHash] =
          Using.resource( conn.prepareStatement( this.SelectForDelivery ) ): ps =>
            ps.setInt( 1, batchSize )
            Using.resource( ps.executeQuery() ): rs =>
              toSet(rs): rs =>
                MailSpec.WithHash(
                  rs.getLong(1),
                  Hash.SHA3_256.withHexBytes( rs.getString(2) ),
                  AddressHeader[From](rs.getString(3)),
                  Option(rs.getString(4)).map( AddressHeader.apply[ReplyTo] ),
                  AddressHeader[To](rs.getString(5)),
                  rs.getString(6),
                  TemplateParams(rs.getString(7)),
                  rs.getInt(8)
                )
        def insert( conn : Connection, hash : Hash.SHA3_256, from : AddressHeader[From], replyTo : Option[AddressHeader[ReplyTo]], to : AddressHeader[To], subject : String, templateParams : TemplateParams, retried : Int ) =
          Using.resource( conn.prepareStatement( this.Insert ) ): ps =>
            ps.setString         (1, hash.hex)
            ps.setString         (2, from.str)
            setStringOptional(ps, 3, Types.VARCHAR, replyTo.map(_.str))
            ps.setString         (4, to.str)
            ps.setString         (5, subject)
            ps.setString         (6, templateParams.toString())
            ps.setInt            (7, retried)
            ps.executeUpdate()
        def insertBatch( conn : Connection, hash : Hash.SHA3_256, from : AddressHeader[From], replyTo : Option[AddressHeader[ReplyTo]], tosWithTemplateParams : Set[(AddressHeader[To],TemplateParams)], subject : String, retried : Int ) =
          Using.resource( conn.prepareStatement( this.Insert ) ): ps =>
            tosWithTemplateParams.foreach: (to, templateParams) =>
              ps.setString         (1, hash.hex)
              ps.setString         (2, from.str)
              setStringOptional(ps, 3, Types.VARCHAR, replyTo.map(_.str))
              ps.setString         (4, to.str)
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
      object ImmediatelyMailable extends Creatable:
        protected val Create =
          """|CREATE TABLE immediately_mailable (
             |  seqnum          BIGINT,
             |  contents        TEXT         NOT NULL,
             |  mail_from       VARCHAR(256) NOT NULL,
             |  reply_to        VARCHAR(256),
             |  mail_to         VARCHAR(256) NOT NULL,
             |  template_params TEXT         NOT NULL,
             |  subject         VARCHAR(256) NOT NULL,
             |  PRIMARY KEY(seqnum)
             |)""".stripMargin
        private val SelectNext =
          """|SELECT seqnum, contents, mail_from, reply_to, mail_to, template_params, subject
             |FROM immediately_mailable
             |ORDER BY seqnum ASC
             |LIMIT 1""".stripMargin
        private val Insert =
          """|INSERT INTO immediately_mailable( seqnum, contents, mail_from, reply_to, mail_to, template_params, subject )
             |VALUES( nextval('immediately_mailable_seq'), ?, ?, ?, ?, ?, ? )""".stripMargin
        private val DeleteByKey = "DELETE FROM immediately_mailable WHERE seqnum = ?"
        def insert( conn : Connection, contents : String, from : AddressHeader[From], replyTo : Option[AddressHeader[ReplyTo]], to : AddressHeader[To], templateParams : TemplateParams, subject : String ) =
          Using.resource( conn.prepareStatement(Insert) ): ps =>
            ps.setString         (1, contents)
            ps.setString         (2, from.str)
            setStringOptional(ps, 3, Types.VARCHAR, replyTo.map( _.str ))
            ps.setString         (4, to.str)
            ps.setString         (5, templateParams.toString())
            ps.setString         (6, subject)
            ps.executeUpdate()
        def selectNext( conn : Connection ) : Option[ImmediateMail] =
          Using.resource( conn.prepareStatement(SelectNext) ): ps =>
            Using.resource( ps.executeQuery() ): rs =>
              zeroOrOneResult("immediately-mailable-select-next", rs): rs =>
                ImmediateMail(
                  rs.getLong(1),
                  rs.getString(2),
                  AddressHeader[From](rs.getString(3)),
                  Option(rs.getString(4)).map(AddressHeader.apply[ReplyTo]),
                  AddressHeader[To](rs.getString(5)),
                  TemplateParams(rs.getString(6)),
                  rs.getString(7)
                )
        def delete( conn : Connection, im : ImmediateMail ) =
          Using.resource( conn.prepareStatement(DeleteByKey) ): ps =>
            ps.setLong(1, im.seqnum)
            ps.executeUpdate()
        object Sequence:
          object ImmediatelyMailableSeq extends Creatable:
            protected val Create = "CREATE SEQUENCE immediately_mailable_seq AS BIGINT"

      // Mastodon destinations are usually few, so we're not going to bother
      // sharing content-addressed templates like we did for mailing
      object MastoPostable extends Creatable:
        protected val Create =
          """|CREATE TABLE masto_postable(
             |  seqnum          BIGINT,
             |  final_content   TEXT NOT NULL,
             |  instance_url    VARCHAR(1024) NOT NULL,
             |  name            VARCHAR(256)  NOT NULL,
             |  retried         INTEGER       NOT NULL,
             |  PRIMARY KEY(seqnum)
             |)""".stripMargin
        private val Insert =
          """|INSERT INTO masto_postable(seqnum, final_content, instance_url, name, retried)
             |VALUES ( ?, ?, ?, ?, ? )""".stripMargin
        private val Delete =
          """|DELETE FROM masto_postable
             |WHERE seqnum = ?""".stripMargin
        private val SelectById =
          """|SELECT final_content, instance_url, name, retried
             |FROM masto_postable
             |WHERE seqnum = ?""".stripMargin
        private val SelectAll =
          """|SELECT seqnum, final_content, instance_url, name, retried
             |FROM masto_postable""".stripMargin
        def insert( conn : Connection, id : MastoPostableId, finalContent : String, mastoInstanceUrl : MastoInstanceUrl, mastoName : MastoName, retried : Int ) =
          Using.resource( conn.prepareStatement( Insert ) ): ps =>
            ps.setLong  (1, id.toLong)
            ps.setString(2, finalContent)
            ps.setString(3, mastoInstanceUrl.str)
            ps.setString(4, mastoName.str)
            ps.setInt   (5, retried)
            val rowsInserted = ps.executeUpdate()
            TRACE.log(s"Inserted into masto_postable, seqnum ${id.toLong}, ${rowsInserted} rows inserted.")
        def delete( conn : Connection, id : MastoPostableId ) : Boolean =
          Using.resource( conn.prepareStatement( Delete ) ): ps =>
            ps.setLong(1, id.toLong)
            val rowsDeleted = ps.executeUpdate()
            TRACE.log(s"Deleted from masto_postable, seqnum ${id.toLong}, ${rowsDeleted} rows deleted.")
            rowsDeleted > 0
        def selectByIdAndMedia( conn : Connection, id : MastoPostableId, media : Seq[ItemContent.Media] ) : Set[MastoPostable] =
          Using.resource( conn.prepareStatement( SelectById ) ): ps =>
            ps.setLong(1, id.toLong)
            Using.resource( ps.executeQuery() ): rs =>
              toSet( rs )( rs => com.mchange.feedletter.MastoPostable( MastoPostableId( id.toLong ), rs.getString(1), MastoInstanceUrl( rs.getString(2) ), MastoName( rs.getString(3) ), rs.getInt(4), media ) )
        def foreach( conn : Connection )( action : MastoPostable => Unit ) =
          Using.resource( conn.prepareStatement(SelectAll) ): ps =>
            Using.resource( ps.executeQuery() ): rs =>
              while rs.next() do
                val id           = MastoPostableId( rs.getLong(1) )
                val finalContent = rs.getString(2)
                val instanceUrl  = MastoInstanceUrl( rs.getString(3) )
                val name         = MastoName( rs.getString(4) )
                val retried      = rs.getInt(5)
                val media        = MastoPostableMedia.selectAllForId(conn, id)
                action( com.mchange.feedletter.MastoPostable( id, finalContent, instanceUrl, name, retried, media ) )
        def all( conn : Connection ) : Set[MastoPostable] =
          val proto = Set.newBuilder[MastoPostable]
          foreach( conn )( mp => proto += mp )
          proto.result()
        object Sequence:
          object MastoPostableSeq extends Creatable:
            protected val Create = "CREATE SEQUENCE masto_postable_seq AS BIGINT"
              private val SelectNext = "SELECT nextval('masto_postable_seq')"
              def selectNext( conn : Connection ) : MastoPostableId =
                Using.resource( conn.prepareStatement(SelectNext) ): ps =>
                  Using.resource( ps.executeQuery() ): rs =>
                    uniqueResult("select-next-masto-postable-seq", rs)( rs => MastoPostableId( rs.getLong(1) ) )
      object MastoPostableMedia extends Creatable:
        protected val Create =
          """|CREATE TABLE masto_postable_media (
             |  masto_postable_id  BIGINT,
             |  position           INT,
             |  media_url          VARCHAR(1024) NOT NULL,
             |  mime_type          VARCHAR(256),
             |  size               BIGINT,
             |  alt                TEXT,
             |  PRIMARY KEY(masto_postable_id, position),
             |  FOREIGN KEY(masto_postable_id) REFERENCES masto_postable(seqnum)
             |)""".stripMargin
        private val Insert =
          """|INSERT INTO masto_postable_media(masto_postable_id,position,media_url,mime_type,size,alt)
             |VALUES( ?, ?, ?, ?, ?, ? )""".stripMargin
        private val SelectAllForId =
          """|SELECT media_url, mime_type, size, alt
             |FROM masto_postable_media
             |WHERE masto_postable_id = ?
             |ORDER BY position""".stripMargin
        private val DeleteById =
          """|DELETE FROM masto_postable_media
             |WHERE masto_postable_id = ?""".stripMargin
        def insert( conn : Connection, id : MastoPostableId, position : Int, media : ItemContent.Media ) =
          Using.resource( conn.prepareStatement(Insert) ): ps =>
            ps.setLong            (1, id.toLong )
            ps.setInt             (2, position )
            ps.setString          (3, media.url )
            setStringOptional( ps, 4, Types.VARCHAR, media.mimeType )
            setLongOptional  ( ps, 5, Types.BIGINT, media.length )
            setStringOptional( ps, 6, Types.CLOB, media.alt )
            ps.executeUpdate()
        def selectAllForId( conn : Connection, id : MastoPostableId ) : Seq[ItemContent.Media] =
          Using.resource( conn.prepareStatement(SelectAllForId) ): ps =>
            ps.setLong(1, id.toLong)
            Using.resource( ps.executeQuery() ): rs =>
              val builder = Seq.newBuilder[ItemContent.Media]
              while rs.next do
                builder += ItemContent.Media( rs.getString(1), Option( rs.getString(2) ), Option( rs.getLong(3) ), Option( rs.getString(4) ) )
              builder.result  
        def deleteById( conn : Connection, id : MastoPostableId ) =
          Using.resource( conn.prepareStatement( DeleteById ) ): ps =>
            ps.setLong(1, id.toLong )
            ps.executeUpdate()
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
            ps.setString(1, subscribableName.str)
            Using.resource( ps.executeQuery() ): rs =>
              uniqueResult("select-feed-id-url-for-subname", rs): rs =>
                ( FeedId( rs.getInt(1) ), FeedUrl( rs.getString(2) ) )
        private val SelectFeedUrlSubscriptionManagerForSubscribableName =
          """|SELECT url, subscription_manager_json
             |FROM feed
             |INNER JOIN subscribable
             |ON feed.id = subscribable.feed_id
             |WHERE subscribable.subscribable_name = ?""".stripMargin
        def selectFeedUrlSubscriptionManagerForSubscribableName( conn : Connection, subscribableName : SubscribableName ) : ( FeedUrl, SubscriptionManager ) =
          Using.resource( conn.prepareStatement( SelectFeedUrlSubscriptionManagerForSubscribableName ) ): ps =>
            ps.setString(1, subscribableName.str)
            Using.resource( ps.executeQuery() ): rs =>
              uniqueResult("select-feed-url-subscription-type-for-subname", rs): rs =>
                ( FeedUrl( rs.getString(1) ), SubscriptionManager.materialize( SubscriptionManager.Json(rs.getString(2)) ) )
      end ItemSubscribable
      object ItemAssignment:
        // since multiple feeds can share the same GUIDs (composite feeds, feeds with the same source, but different timings), it's important to specify the feed ID!
        private val SelectItemContentsForAssignable =
          """|SELECT item.guid, single_item_rss
             |FROM item
             |INNER JOIN assignment
             |ON item.guid = assignment.guid
             |WHERE item.feed_id = ? AND assignment.subscribable_name = ? AND assignment.within_type_id = ?
             |ORDER BY item.first_seen DESC""".stripMargin
        def selectItemContentsForAssignable( conn : Connection, feedId : FeedId, subscribableName : SubscribableName, withinTypeId : String ) : Seq[ItemContent] =
          Using.resource( conn.prepareStatement( SelectItemContentsForAssignable ) ): ps =>
            ps.setInt   (1, feedId.toInt)
            ps.setString(2, subscribableName.str)
            ps.setString(3, withinTypeId)
            Using.resource( ps.executeQuery() ): rs =>
              toSeq( rs )( rs => ItemContent.fromPrenormalizedSingleItemRss( rs.getString(1), rs.getString(2) ) )
      end ItemAssignment
      object ItemAssignableAssignment:
        private val SelectLiveAssignedGuids =
           """|SELECT guid
              |FROM assignable
              |INNER JOIN assignment
              |ON assignable.subscribable_name = assignment.subscribable_name AND assignable.within_type_id = assignment.within_type_id""".stripMargin
        private val ClearOldCache =
           s"""|UPDATE item
               |SET single_item_rss = NULL, content_hash = NULL, assignability = 'Cleared'      -- note that we leave link alone...
               |WHERE assignability = 'Assigned' AND NOT item.guid IN ( ${SelectLiveAssignedGuids} )""".stripMargin
        def clearOldCache( conn : Connection ) =
          Using.resource( conn.prepareStatement( ClearOldCache ) )( _.executeUpdate() )
      end ItemAssignableAssignment
      object SubscribableSubscription:
        private val SelectSubscriptionInfoForSubscriptionId =
          """|SELECT subscription.subscription_id, subscribable.subscribable_name, subscription_manager_json, destination_json, subscription.confirmed
             |FROM subscribable
             |INNER JOIN subscription
             |ON subscribable.subscribable_name = subscription.subscribable_name
             |WHERE subscription_id = ?""".stripMargin
        def selectSubscriptionInfoForSubscriptionId( conn : Connection, subscriptionId : SubscriptionId ) : Option[SubscriptionInfo] =
          Using.resource( conn.prepareStatement( SelectSubscriptionInfoForSubscriptionId ) ): ps =>
            ps.setLong(1, subscriptionId.toLong)
            Using.resource( ps.executeQuery() ): rs =>
              zeroOrOneResult("select-sub-manager-for-sub-id", rs): rs =>
                val sid = SubscriptionId( rs.getLong(1) )
                val sname = SubscribableName( rs.getString(2) )
                val sman = SubscriptionManager.materialize( SubscriptionManager.Json( rs.getString(3) ) )
                val dest = Destination.materialize( Destination.Json( rs.getString(4) ) )
                val confirmed = rs.getBoolean(5)
                SubscriptionInfo( sid, sname, sman, dest, confirmed )
      end SubscribableSubscription
    end Join

  object V2 extends Schema:
    override val Version = 2
    object Table:
      val Config              = V1.Table.Config
      val Flags               = V1.Table.Flags
      val Feed                = V1.Table.Feed
      val Item                = V1.Table.Item
      val Subscribable        = V1.Table.Subscribable
      val Assignable          = V1.Table.Assignable
      val Assignment          = V1.Table.Assignment
      val Subscription        = V1.Table.Subscription
      val MailableTemplate    = V1.Table.MailableTemplate
      val Mailable            = V1.Table.Mailable
      val ImmediatelyMailable = V1.Table.ImmediatelyMailable
      val MastoPostable       = V1.Table.MastoPostable
      val MastoPostableMedia  = V1.Table.MastoPostableMedia

      // BlueSky destination, like Mastodon destinations, are usually few, so we're not going to bother
      // sharing content-addressed templates like we did for mailing
      object BskyPostable extends Creatable:
        protected val Create =
          """|CREATE TABLE bsky_postable(
             |  seqnum          BIGINT,
             |  final_content   TEXT NOT NULL,
             |  entryway_url    VARCHAR(1024) NOT NULL,
             |  identifier      VARCHAR(256)  NOT NULL,
             |  retried         INTEGER       NOT NULL,
             |  PRIMARY KEY(seqnum)
             |)""".stripMargin
        private val Insert =
          """|INSERT INTO bsky_postable(seqnum, final_content, entryway_url, identifier, retried)
             |VALUES ( ?, ?, ?, ?, ? )""".stripMargin
        private val Delete =
          """|DELETE FROM bsky_postable
             |WHERE seqnum = ?""".stripMargin
        private val SelectById =
          """|SELECT final_content, entryway_url, identifier, retried
             |FROM bsky_postable
             |WHERE seqnum = ?""".stripMargin
        private val SelectAll =
          """|SELECT seqnum, final_content, entryway_url, identifier, retried
             |FROM bsky_postable""".stripMargin
        def insert( conn : Connection, id : BskyPostableId, finalContent : String, bskyEntrywayUrl : BskyEntrywayUrl, bskyIdentifier : BskyIdentifier, retried : Int ) =
          Using.resource( conn.prepareStatement( Insert ) ): ps =>
            ps.setLong  (1, id.toLong)
            ps.setString(2, finalContent)
            ps.setString(3, bskyEntrywayUrl.str)
            ps.setString(4, bskyIdentifier.str)
            ps.setInt   (5, retried)
            val rowsInserted = ps.executeUpdate()
            TRACE.log(s"Inserted into bsky_postable, seqnum ${id.toLong}, ${rowsInserted} rows inserted.")
        def delete( conn : Connection, id : BskyPostableId ) : Boolean =
          Using.resource( conn.prepareStatement( Delete ) ): ps =>
            ps.setLong(1, id.toLong)
            val rowsDeleted = ps.executeUpdate()
            TRACE.log(s"Deleted from bsky_postable, seqnum ${id.toLong}, ${rowsDeleted} rows deleted.")
            rowsDeleted > 0
        def selectByIdAndMedia( conn : Connection, id : BskyPostableId, media : Seq[ItemContent.Media] ) : Set[BskyPostable] =
          Using.resource( conn.prepareStatement( SelectById ) ): ps =>
            ps.setLong(1, id.toLong)
            Using.resource( ps.executeQuery() ): rs =>
              toSet( rs )( rs => com.mchange.feedletter.BskyPostable( BskyPostableId( id.toLong ), rs.getString(1), BskyEntrywayUrl( rs.getString(2) ), BskyIdentifier( rs.getString(3) ), rs.getInt(4), media ) )
        def foreach( conn : Connection )( action : BskyPostable => Unit ) =
          Using.resource( conn.prepareStatement(SelectAll) ): ps =>
            Using.resource( ps.executeQuery() ): rs =>
              while rs.next() do
                val id           = BskyPostableId( rs.getLong(1) )
                val finalContent = rs.getString(2)
                val entrywayUrl  = BskyEntrywayUrl( rs.getString(3) )
                val identifier   = BskyIdentifier( rs.getString(4) )
                val retried      = rs.getInt(5)
                val media        = BskyPostableMedia.selectAllForId(conn, id)
                action( com.mchange.feedletter.BskyPostable( id, finalContent, entrywayUrl, identifier, retried, media ) )
        def all( conn : Connection ) : Set[BskyPostable] =
          val proto = Set.newBuilder[BskyPostable]
          foreach( conn )( mp => proto += mp )
          proto.result()
        object Sequence:
          object BskyPostableSeq extends Creatable:
            protected val Create = "CREATE SEQUENCE bsky_postable_seq AS BIGINT"
            private val SelectNext = "SELECT nextval('bsky_postable_seq')"
            def selectNext( conn : Connection ) : BskyPostableId =
              Using.resource( conn.prepareStatement(SelectNext) ): ps =>
                Using.resource( ps.executeQuery() ): rs =>
                  uniqueResult("select-next-bsky-postable-seq", rs)( rs => BskyPostableId( rs.getLong(1) ) )
      object BskyPostableMedia extends Creatable:
        protected val Create =
          """|CREATE TABLE bsky_postable_media (
             |  bsky_postable_id   BIGINT,
             |  position           INT,
             |  media_url          VARCHAR(1024) NOT NULL,
             |  mime_type          VARCHAR(256),
             |  size               BIGINT,
             |  alt                TEXT,
             |  PRIMARY KEY(bsky_postable_id, position),
             |  FOREIGN KEY(bsky_postable_id) REFERENCES bsky_postable(seqnum)
             |)""".stripMargin
        private val Insert =
          """|INSERT INTO bsky_postable_media(bsky_postable_id,position,media_url,mime_type,size,alt)
             |VALUES( ?, ?, ?, ?, ?, ? )""".stripMargin
        private val SelectAllForId =
          """|SELECT media_url, mime_type, size, alt
             |FROM bsky_postable_media
             |WHERE bsky_postable_id = ?
             |ORDER BY position""".stripMargin
        private val DeleteById =
          """|DELETE FROM bsky_postable_media
             |WHERE bsky_postable_id = ?""".stripMargin
        def insert( conn : Connection, id : BskyPostableId, position : Int, media : ItemContent.Media ) =
          Using.resource( conn.prepareStatement(Insert) ): ps =>
            ps.setLong            (1, id.toLong )
            ps.setInt             (2, position )
            ps.setString          (3, media.url )
            setStringOptional( ps, 4, Types.VARCHAR, media.mimeType )
            setLongOptional  ( ps, 5, Types.BIGINT, media.length )
            setStringOptional( ps, 6, Types.CLOB, media.alt )
            ps.executeUpdate()
        def selectAllForId( conn : Connection, id : BskyPostableId ) : Seq[ItemContent.Media] =
          Using.resource( conn.prepareStatement(SelectAllForId) ): ps =>
            ps.setLong(1, id.toLong)
            Using.resource( ps.executeQuery() ): rs =>
              val builder = Seq.newBuilder[ItemContent.Media]
              while rs.next do
                builder += ItemContent.Media( rs.getString(1), Option( rs.getString(2) ), Option( rs.getLong(3) ), Option( rs.getString(4) ) )
              builder.result  
        def deleteById( conn : Connection, id : BskyPostableId ) =
          Using.resource( conn.prepareStatement( DeleteById ) ): ps =>
            ps.setLong(1, id.toLong )
            ps.executeUpdate()
    val Join = V1.Join  
