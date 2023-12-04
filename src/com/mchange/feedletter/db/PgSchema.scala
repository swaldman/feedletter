package com.mchange.feedletter.db

import java.sql.{Connection,Statement,Timestamp,Types}
import java.time.Instant
import scala.util.Using

import com.mchange.feedletter.{ConfigKey,ExcludedItem,FeedInfo,ItemContent,SubscriptionType}

object PgSchema:
  trait Creatable:
    def Create : String
    def create( stmt : Statement ) : Int = stmt.executeUpdate( this.Create )
    def create( conn : Connection ) : Int = Using.resource( conn.createStatement() )( stmt => create(stmt) )

  object Unversioned:
    object Table:
      object Metadata extends Creatable:
        val Name = "metadata"
        val Create = "CREATE TABLE metadata( key VARCHAR(64) PRIMARY KEY, value VARCHAR(64) NOT NULL )"
        val Insert = "INSERT INTO metadata(key, value) VALUES( ?, ? )"
        val Update = "UPDATE metadata SET value = ? WHERE key = ?"
        val Select = "SELECT value FROM metadata WHERE key = ?"
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
          val Create = "CREATE TABLE config( key VARCHAR(1024) PRIMARY KEY, value VARCHAR(1024) NOT NULL )"
          val Insert = "INSERT INTO config(key, value) VALUES( ?, ? )"
          val Update = "UPDATE config SET value = ? WHERE key = ?"
          val Select = "SELECT value FROM config WHERE key = ?"
          val SelectTuples = "SELECT key, value FROM config"
          val Upsert =
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
          val Create =
            """|CREATE TABLE feed(
               |  url VARCHAR(1024),
               |  min_delay_minutes INTEGER,
               |  await_stabilization_minutes INTEGER,
               |  max_delay_minutes INTEGER,
               |  paused BOOLEAN,
               |  subscribed TIMESTAMP,
               |  PRIMARY KEY(url)
               |)""".stripMargin
          val Insert =
            """|INSERT INTO feed(url, min_delay_minutes, await_stabilization_minutes, max_delay_minutes, paused, subscribed)
               |VALUES( ?, ?, ?, ?, ?, ? )""".stripMargin
          val Select =
            "SELECT url, min_delay_minutes, await_stabilization_minutes, max_delay_minutes, paused, subscribed FROM feed"
          val Upsert =
            """|INSERT INTO feed(url, min_delay_minutes, await_stabilization_minutes, max_delay_minutes, paused, subscribed)
               |VALUES ( ?, ?, ?, ?, ?, ? )
               |ON CONFLICT(url) DO UPDATE
               |SET min_delay_minutes = ?, await_stabilization_minutes = ?, max_delay_minutes = ?, paused = ?""".stripMargin // leave subscribed as first set
          def insert( conn : Connection, fi : FeedInfo ) : Int =
            insert(conn, fi.feedUrl, fi.minDelayMinutes, fi.awaitStabilizationMinutes, fi.maxDelayMinutes, fi.paused, fi.subscribed)
          def insert( conn : Connection, url : String, minDelayMinutes : Int, awaitStabilizationMinutes : Int, maxDelayMinutes : Int, paused : Boolean, subscribed : Instant ) : Int =
            Using.resource(conn.prepareStatement(this.Insert)): ps =>
              ps.setString(1, url)
              ps.setInt(2, minDelayMinutes )
              ps.setInt(3, awaitStabilizationMinutes)
              ps.setInt(4, maxDelayMinutes )
              ps.setBoolean(5, paused)
              ps.setTimestamp( 6, Timestamp.from(subscribed) )
              ps.executeUpdate()
          def select( conn : Connection ) : Set[FeedInfo] =
            Using.resource( conn.prepareStatement( this.Select ) ): ps =>
              Using.resource( ps.executeQuery() ): rs =>
                toSet(rs)( rs => FeedInfo( rs.getString(1), rs.getInt(2), rs.getInt(3), rs.getInt(4), rs.getBoolean(5), rs.getTimestamp(6).toInstant ) )
          def upsert( conn : Connection, fi : FeedInfo ) =
            Using.resource( conn.prepareStatement(this.Upsert) ): ps =>
              ps.setString(1, fi.feedUrl)
              ps.setInt(2, fi.minDelayMinutes )
              ps.setInt(3, fi.awaitStabilizationMinutes)
              ps.setInt(4, fi.maxDelayMinutes )
              ps.setBoolean(5, fi.paused)
              ps.setTimestamp(6, Timestamp.from(fi.subscribed))
              ps.setInt(7, fi.minDelayMinutes )
              ps.setInt(8, fi.awaitStabilizationMinutes)
              ps.setInt(9, fi.maxDelayMinutes )
              ps.setBoolean(10, fi.paused)
              ps.executeUpdate()

        object Item extends Creatable:
          object Type:
            object ItemAssignability extends Creatable:
              val Create = "CREATE TYPE ItemAssignability AS ENUM ('Unassigned', 'Assigned', 'Excluded')"
          val Create =
            """|CREATE TABLE item(
               |  feed_url VARCHAR(1024),
               |  guid VARCHAR(1024),
               |  title VARCHAR(1024),
               |  author VARCHAR(1024),
               |  article TEXT,
               |  publication_date TIMESTAMP,
               |  link VARCHAR(1024),
               |  content_hash INTEGER NOT NULL, -- ItemContent.## (hashCode)
               |  first_seen TIMESTAMP NOT NULL,
               |  last_checked TIMESTAMP NOT NULL,
               |  stable_since TIMESTAMP NOT NULL,
               |  assignability ItemAssignability NOT NULL,
               |  PRIMARY KEY(feed_url, guid),
               |  FOREIGN KEY(feed_url) REFERENCES feed(url)
               |)""".stripMargin
          val SelectCheck =
            """|SELECT content_hash, first_seen, last_checked, stable_since, assignability
               |FROM item
               |WHERE feed_url = ? AND guid = ?""".stripMargin
          val SelectExcluded =
            s"""|SELECT feed_url, guid, title, author, publication_date, link
                |FROM item
                |WHERE assignability = '${ItemAssignability.Excluded}'""".stripMargin
          val Insert =
            """|INSERT INTO item(feed_url, guid, title, author, article, publication_date, link, content_hash, first_seen, last_checked, stable_since, assignability)
               |VALUES( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CAST( ? AS ItemAssignability ) )""".stripMargin
          val UpdateChanged =
            """|UPDATE item
               |SET title = ?, author = ?, article = ?, publication_date = ?, link = ?, content_hash = ?, last_checked = ?, stable_since = ?, assignability = CAST( ? AS ItemAssignability )
               |WHERE feed_url = ? AND guid = ?""".stripMargin
          val UpdateStable =
            """|UPDATE item
               |SET last_checked = ?
               |WHERE feed_url = ? AND guid = ?""".stripMargin
          val UpdateAssignability =
            """|UPDATE item
               |SET assignability = CAST( ? AS ItemAssignability )
               |WHERE feed_url = ? AND guid = ?""".stripMargin
          def checkStatus( conn : Connection, feedUrl : String, guid : String ) : Option[ItemStatus] =
            Using.resource( conn.prepareStatement( SelectCheck ) ): ps =>
              ps.setString(1, feedUrl)
              ps.setString(2, guid)
              Using.resource( ps.executeQuery() ): rs =>
                zeroOrOneResult("item-check-select", rs): rs =>
                  ItemStatus( rs.getInt(1), rs.getTimestamp(2).toInstant(), rs.getTimestamp(3).toInstant(), rs.getTimestamp(4).toInstant(), ItemAssignability.valueOf(rs.getString(5)) )
          def selectExcluded( conn : Connection ) : Set[ExcludedItem] =
            Using.resource( conn.prepareStatement(SelectExcluded) ): ps =>
              Using.resource( ps.executeQuery() ): rs =>
                toSet(rs)( rs => ExcludedItem( rs.getString(1), rs.getString(2), Option( rs.getString(3) ), Option( rs.getString(4) ), Option( rs.getTimestamp(5) ).map( _.toInstant ), Option( rs.getString(6) ) ) )
          def updateStable( conn : Connection, feedUrl : String, guid : String, lastChecked : Instant ) =
            Using.resource( conn.prepareStatement( this.UpdateStable) ): ps =>
              ps.setTimestamp(1, Timestamp.from(lastChecked))
              ps.setString(2, feedUrl)
              ps.setString(3, guid)
              ps.executeUpdate()
          def updateChanged( conn : Connection, feedUrl : String, guid : String, newContent : ItemContent, newStatus : ItemStatus ) =
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
              ps.setString(10, feedUrl)
              ps.setString(11, guid)
              ps.executeUpdate()
          def updateAssignability( conn : Connection, feedUrl : String, guid : String, assignability : ItemAssignability ) =
            Using.resource( conn.prepareStatement( this.UpdateAssignability) ): ps =>
              ps.setString(1, assignability.toString())
              ps.setString(2, feedUrl)
              ps.setString(3, guid)
              ps.executeUpdate()
          def insertNew( conn : Connection, feedUrl : String, guid : String, itemContent : ItemContent, assignability : ItemAssignability ) : Int =
            Using.resource( conn.prepareStatement( Insert ) ): ps =>
              val now = Instant.now
              ps.setString( 1, feedUrl )
              ps.setString( 2, guid )
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
        object SubscriptionType extends Creatable:
          val Create = "CREATE TABLE subscription_type( stype VARCHAR(32) PRIMARY KEY )"
          val Insert = "INSERT INTO subscription_type VALUES ( ? )"
          val Ensure = "INSERT INTO subscription_type VALUES ( ? ) ON CONFLICT(stype) DO NOTHING"
          def insert( conn : Connection, subscriptionType : SubscriptionType, moreSubscriptionTypes : SubscriptionType* ) : Unit =
            Using.resource( conn.prepareStatement( Insert ) ): ps =>
              def insertType( stype : SubscriptionType ) =
                ps.setString(1, stype.toString())
                ps.executeUpdate()
              insertType( subscriptionType )
              moreSubscriptionTypes.foreach( insertType )
          def ensure( conn : Connection, subscriptionType : SubscriptionType, moreSubscriptionTypes : SubscriptionType* ) : Unit =
            Using.resource( conn.prepareStatement( Ensure ) ): ps =>
              def ensureType( stype : SubscriptionType ) =
                ps.setString(1, stype.toString())
                ps.executeUpdate()
              ensureType( subscriptionType )
              moreSubscriptionTypes.foreach( ensureType )
        object Assignable extends Creatable:
          val Create = // an assignable represents a collection of posts for a single mail
            """|CREATE TABLE assignable(
               |  feed_url VARCHAR(1024),
               |  stype VARCHAR(32),
               |  within_type_id VARCHAR(1024),
               |  opened TIMESTAMP NOT NULL,
               |  completed TIMESTAMP,
               |  PRIMARY KEY(feed_url, stype, within_type_id),
               |  FOREIGN KEY(feed_url) REFERENCES feed(url),
               |  FOREIGN KEY(stype) REFERENCES subscription_type(stype)
               |)""".stripMargin
          val SelectIsCompleted =
            """|SELECT completed IS NOT NULL
               |FROM assignable
               |WHERE feed_url = ? AND stype = ? AND within_type_id = ?""".stripMargin
          val SelectOpen =
            """|SELECT feed_url, stype, within_type_id
               |FROM assignable
               |WHERE completed IS NULL""".stripMargin
          // should I make indexes for these next two? should I try some more clever/efficient form of query?
          // see
          //   https://stackoverflow.com/questions/3800551/select-first-row-in-each-group-by-group/7630564#7630564
          //   https://stackoverflow.com/questions/1684244/efficient-latest-record-query-with-postgresql
          //   https://www.timescale.com/blog/select-the-most-recent-record-of-many-items-with-postgresql/
          val SelectWithinTypeIdMostRecentOpen =
            """|SELECT within_type_id
               |FROM assignable
               |WHERE feed_url = ? AND stype = ?
               |ORDER BY opened DESC
               |LIMIT 1""".stripMargin
          val SelectWithinTypeIdLastCompleted =
            """|SELECT within_type_id
               |FROM assignable
               |WHERE feed_url = ? AND stype = ?
               |ORDER BY completed DESC NULLS LAST
               |LIMIT 1""".stripMargin
          val Insert =
            """|INSERT INTO assignable( feed_url, stype, within_type_id, opened, completed )
               |VALUES ( ?, ?, ?, ?, ? )""".stripMargin
          val UpdateCompleted =
            """|UPDATE assignable
               |SET completed = ?
               |WHERE feed_url = ? AND stype = ? AND within_type_id = ?""".stripMargin
          def selectOpen( conn : Connection ) : Set[AssignableKey] =
            Using.resource( conn.prepareStatement( this.SelectOpen ) ): ps =>
              Using.resource( ps.executeQuery() ): rs =>
                toSet(rs)( rs => AssignableKey( rs.getString(1), com.mchange.feedletter.SubscriptionType.parse( rs.getString(2) ), rs.getString(3) ) )
          def selectIsCompleted( conn : Connection, feedUrl : String, stype : SubscriptionType, withinTypeId : String ) : Option[Boolean] =
            Using.resource( conn.prepareStatement( this.SelectIsCompleted ) ): ps =>
              ps.setString(1, feedUrl)
              ps.setString(2, stype.toString())
              ps.setString(3, withinTypeId)
              Using.resource(ps.executeQuery()): rs =>
                zeroOrOneResult("assignable-select-completed", rs)( _.getBoolean(1) )
          def selectWithinTypeIdMostRecentOpen( conn : Connection, feedUrl : String, stype : SubscriptionType ) : Option[String] =
            Using.resource( conn.prepareStatement( this.SelectWithinTypeIdMostRecentOpen ) ): ps =>
              ps.setString(1, feedUrl)
              ps.setString(2, stype.toString())
              Using.resource(ps.executeQuery()): rs =>
                zeroOrOneResult("assignable-select-most-recent-open-within-type-id", rs)( _.getString(1) )
          def selectWithinTypeIdLastCompleted( conn : Connection, feedUrl : String, stype : SubscriptionType ) : Option[String] =
            Using.resource( conn.prepareStatement( this.SelectWithinTypeIdLastCompleted ) ): ps =>
              ps.setString(1, feedUrl)
              ps.setString(2, stype.toString())
              Using.resource(ps.executeQuery()): rs =>
                zeroOrOneResult("assignable-select-last-completed-within-type-id", rs)( _.getString(1) )
          def insert( conn : Connection, feedUrl : String, stype : SubscriptionType, withinTypeId : String, opened : Instant, completed : Option[Instant] ) =
            Using.resource( conn.prepareStatement( this.Insert ) ): ps =>
              ps.setString(1, feedUrl)
              ps.setString(2, stype.toString())
              ps.setString(3, withinTypeId)
              ps.setTimestamp(4, Timestamp.from(opened))
              setTimestampOptional(ps, 5, completed.map( Timestamp.from ))
              ps.executeUpdate()
          def updateCompleted( conn : Connection, feedUrl : String, stype : SubscriptionType, withinTypeId : String, completed : Option[Instant] ) =
            Using.resource( conn.prepareStatement( this.UpdateCompleted ) ): ps =>
              completed.fold( ps.setNull(1, Types.TIMESTAMP) )( instant => ps.setTimestamp(1, Timestamp.from(instant)) )
              ps.setString(2, feedUrl)
              ps.setString(3, stype.toString())
              ps.setString(4, withinTypeId)
              ps.executeUpdate()
        object Assignment extends Creatable:
          val Create = // an assignment represents a membership of a post in a collection
            """|CREATE TABLE assignment(
               |  feed_url VARCHAR(1024),
               |  stype VARCHAR(32),
               |  within_type_id VARCHAR(1024),
               |  guid VARCHAR(1024),
               |  PRIMARY KEY( feed_url, stype, within_type_id, guid ),
               |  FOREIGN KEY( feed_url, guid ) REFERENCES item( feed_url, guid ),
               |  FOREIGN KEY( feed_url, stype, within_type_id ) REFERENCES assignable( feed_url, stype, within_type_id )
               |)""".stripMargin
          val SelectCountWithinAssignable =
            """|SELECT COUNT(*)
               |FROM assignment
               |WHERE feed_url = ? AND stype = ? AND within_type_id = ?""".stripMargin
          val Insert =
            """|INSERT INTO assignment( feed_url, stype, within_type_id, guid )
               |VALUES ( ?, ?, ?, ? )""".stripMargin
          def selectCountWithinAssignable( conn : Connection, feedUrl : String, stype : SubscriptionType, withinTypeId : String ) : Int =
            Using.resource( conn.prepareStatement( this.SelectCountWithinAssignable ) ): ps =>
              ps.setString(1, feedUrl)
              ps.setString(2, stype.toString())
              ps.setString(3, withinTypeId)
              Using.resource( ps.executeQuery() ): rs =>
                uniqueResult( "select-count-within-assignable", rs )( _.getInt(1) )
          def insert( conn : Connection, feedUrl : String, stype : SubscriptionType, withinTypeId : String, guid : String ) =
            Using.resource( conn.prepareStatement( this.Insert ) ): ps =>
              ps.setString(1, feedUrl)
              ps.setString(2, stype.toString())
              ps.setString(3, withinTypeId)
              ps.setString(4, guid)
              ps.executeUpdate()
        object Subscription extends Creatable:
          val Create =
            """|CREATE TABLE subscription(
               |  destination VARCHAR(1024),
               |  feed_url VARCHAR(1024),
               |  stype VARCHAR(64),
               |  PRIMARY KEY( destination, feed_url ),
               |  FOREIGN KEY( feed_url ) REFERENCES feed(url),
               |  FOREIGN KEY( stype ) REFERENCES subscription_type( stype )
               |)""".stripMargin
          val SelectSubscriptionTypesByFeedUrl =
            """|SELECT DISTINCT stype
               |FROM subscription
               |WHERE feed_url = ?""".stripMargin
          val SelectDestination =
            """|SELECT destination
               |FROM subscription
               |WHERE feed_url = ? AND stype = ?""".stripMargin
          val Insert =
            """|INSERT INTO subscription(destination, feed_url, stype)
               |VALUES ( ?, ?, ? )""".stripMargin
          def selectSubscriptionTypeByFeedUrl( conn : Connection, feedUrl : String ) : Set[SubscriptionType] =
            Using.resource( conn.prepareStatement( this.SelectSubscriptionTypesByFeedUrl ) ): ps =>
              ps.setString(1, feedUrl)
              Using.resource( ps.executeQuery() ): rs =>
                toSet(rs)( rs => com.mchange.feedletter.SubscriptionType.parse( rs.getString(1) ) )
          def selectDestination( conn : Connection, feedUrl : String, stype : SubscriptionType ) : Set[String] =
            Using.resource( conn.prepareStatement( this.SelectDestination ) ): ps =>
              ps.setString(1, feedUrl)
              ps.setString(2, stype.toString())
              Using.resource( ps.executeQuery() ): rs =>
                toSet(rs)( rs => rs.getString(1) )
          def insert( conn : Connection, destination : String, feedUrl : String, stype : SubscriptionType ) =
            Using.resource( conn.prepareStatement( Insert ) ): ps =>
              ps.setString(1, destination)
              ps.setString(2, feedUrl)
              ps.setString(3, stype.toString())
              ps.executeUpdate()

        // publication-related tables should be decoupled from, unrelated to the
        // tables above. logically, we should be listening for "completion" above
        // as a kind of event, which would trigger SubscriptionType route potentially
        // in a distinct process with a distinct database
        object MailableContents extends Creatable:
          val Create =
            """|CREATE TABLE mailable_contents(
               |  seqnum INTEGER,
               |  contents TEXT
               |  PRIMARY KEY(seqnum)
               |)""".stripMargin
          val Insert =
            """|INSERT INTO mailable_contents(seqnum, contents)
               |VALUES ( ?, ? )""".stripMargin
          object Sequence:
            object MailableContentsSeq extends Creatable:
              val Create = "CREATE SEQUENCE mailable_contents_seq AS INTEGER"
              val Select = "SELECT nextval('mailable_contents_seq')"
              def select( conn : Connection ) : Int =
                Using.resource( conn.prepareStatement( Select ) ): ps =>
                  Using.resource( ps.executeQuery() ): rs =>
                    uniqueResult("select-next-mailable-contents-seq-value", rs)( _.getInt(1) )
        object Mailable extends Creatable:
          val Create =
            """|CREATE TABLE mailable(
               |  seqnum INTEGER,
               |  contents_seqnum INTEGER,
               |  email VARCHAR(256),
               |  mailed BOOLEAN,
               |  PRIMARY KEY(seqnum),
               |  FOREIGN KEY(contents_seqnum) REFERENCES mailable_contents(seqnum)
               |)""".stripMargin
          val Insert =
            """|INSERT INTO mailable(seqnum, contents_seqnum, email, mailed)
               |VALUES ( nextval('mailable_seq'), ?, ?, ? )""".stripMargin
          def insert( conn : Connection, contentsSeqnum : Int, email : String, mailed : Boolean ) =
            Using.resource( conn.prepareStatement( this.Insert ) ): ps =>
              ps.setInt(1, contentsSeqnum)
              ps.setString(2, email)
              ps.setBoolean(3, mailed)
              ps.executeUpdate()
          object Sequence:
            object MailableSeq extends Creatable:
              val Create = "CREATE SEQUENCE mailable_seq AS INTEGER"
      end Table
      object Join:
        object ItemAssignment:
          val SelectItemContentsForAssignable =
            """|SELECT title, author, article, publication_date, link
               |FROM item
               |INNER JOIN assignment
               |ON item.feed_url = assignment.feed_url AND item.guid = assignment.guid
               |WHERE item.feed_url = ? AND assignment.stype = ? AND assignment.within_type_id = ?""".stripMargin
          def selectItemContentsForAssignable( conn : Connection, feedUrl : String, stype : SubscriptionType, withinTypeId : String ) : Set[ItemContent] =
            Using.resource( conn.prepareStatement( SelectItemContentsForAssignable ) ): ps =>
              ps.setString(1, feedUrl)
              ps.setString(2, stype.toString)
              ps.setString(3, withinTypeId)
              Using.resource( ps.executeQuery() ): rs =>
                toSet( rs )( rs => ItemContent( Option( rs.getString(1) ), Option( rs.getString(2) ), Option( rs.getString(3) ), Option( rs.getTimestamp(4) ).map( _.toInstant ), Option( rs.getString(5) ) ) )
        end ItemAssignment
      end Join

