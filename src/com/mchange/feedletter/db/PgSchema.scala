package com.mchange.feedletter.db

import java.sql.{Connection,Statement,Timestamp,Types}
import java.time.Instant
import scala.util.Using

import com.mchange.feedletter.ItemContent

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
        val Insert = "INSERT INTO metadata VALUES( ?, ? )"
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
        object Feed extends Creatable:
          val Create = "CREATE TABLE feed( url VARCHAR(1024) PRIMARY KEY )"
        object Item extends Creatable:  
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
               |  last_checked TIMESTAMP NOT NULL,
               |  stable_since TIMESTAMP NOT NULL,
               |  assigned BOOLEAN NOT NULL,
               |  PRIMARY KEY(feed_url, guid),
               |  FOREIGN KEY(feed_url) REFERENCES feed(url)
               |)""".stripMargin
          val SelectCheck =
            """|SELECT content_hash, last_checked, stable_since, assigned
               |FROM item
               |WHERE feed_url = ? AND guid = ?""".stripMargin
          val Insert =
            """|INSERT INTO item(feed_url, guid, title, author, article, publication_date, link, content_hash, last_checked, stable_since, assigned)
               |VALUES( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )""".stripMargin
          val UpdateChanged =
            """|UPDATE item
               |SET title = ?, author = ?, article = ?, publication_date = ?, link = ?, content_hash = ?, last_checked = ?, stable_since = ?, assigned = ?
               |WHERE feed_url = ? AND guid = ?""".stripMargin
          val UpdateStable =
            """|UPDATE item
               |SET last_checked = ?, assigned = ?
               |WHERE feed_url = ? AND guid = ?""".stripMargin
          def checkStatus( conn : Connection, feedUrl : String, guid : String ) : Option[ItemStatus] =
            Using.resource( conn.prepareStatement( SelectCheck ) ): ps =>
              ps.setString(1, feedUrl)
              ps.setString(2, guid)
              Using.resource( ps.executeQuery() ): rs =>
                zeroOrOneResult("item-check-select", rs): rs =>
                  ItemStatus( rs.getInt(1), rs.getTimestamp(2).toInstant(), rs.getTimestamp(3).toInstant(), rs.getBoolean(4) )
          def insertNew( conn : Connection, feedUrl : String, guid : String, itemContent : ItemContent ) : Int =
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
              ps.setBoolean( 11, false )
              ps.executeUpdate()
               
        object SubscriptionType extends Creatable:       
          val Create = "CREATE TABLE subscription_type( stype VARCHAR(32) PRIMARY KEY )"
          val Insert = "INSERT INTO subscription_type VALUES ( ? )"
          def insert( conn : Connection, subscriptionType : SubscriptionType, moreSubscriptionTypes : SubscriptionType* ) : Unit =
            Using.resource( conn.prepareStatement( Insert ) ): ps =>
              def insertType( stype : SubscriptionType ) =
                ps.setString(1, stype.toString())
                ps.executeUpdate()
              insertType( subscriptionType )
              moreSubscriptionTypes.foreach( insertType )
        object Assignable extends Creatable:  
          val Create = // an assignable represents a collection of posts for a single mail
            """|CREATE TABLE assignable(
               |  feed_url VARCHAR(1024),
               |  stype VARCHAR(32),
               |  within_type_id VARCHAR(1024),
               |  completed BOOLEAN,
               |  PRIMARY KEY(feed_url, stype, within_type_id),
               |  FOREIGN KEY(feed_url) REFERENCES feed(url),
               |  FOREIGN KEY(stype) REFERENCES subscription_type(stype)
               |)""".stripMargin
          val SelectCompleted =
            """|SELECT completed
               |FROM assignable
               |WHERE feed_url = ? AND stype = ? AND within_type_id = ?""".stripMargin
          val Insert =
            """|INSERT INTO assignable( feed_url, stype, within_type_id, completed )
               |VALUES ( ?, ?, ?, ? )""".stripMargin
          def selectCompleted( conn : Connection, feedUrl : String, stype : SubscriptionType, withinTypeId : String ) : Option[Boolean] =
            Using.resource( conn.prepareStatement( SelectCompleted ) ): ps =>
              ps.setString(1, feedUrl)
              ps.setString(2, stype.toString())
              ps.setString(3, withinTypeId)
              Using.resource(ps.executeQuery()): rs =>
                zeroOrOneResult("assignable-select-completed", rs)( _.getBoolean(1) )
          def insert( conn : Connection, feedUrl : String, stype : SubscriptionType, withinTypeId : String, completed : Boolean ) =
            Using.resource( conn.prepareStatement( this.Insert ) ): ps =>
              ps.setString(1, feedUrl)
              ps.setString(2, stype.toString())
              ps.setString(3, withinTypeId)
              ps.setBoolean(4, completed)
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
          val Insert =
            """|INSERT INTO assignment( feed_url, stype, within_type_id, guid )
               |VALUES ( ?, ?, ?, ? )""".stripMargin
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
               |  email VARCHAR(256),
               |  feed_url VARCHAR(256),
               |  stype VARCHAR(32),
               |  PRIMARY KEY( email, feed_url ),
               |  FOREIGN KEY( feed_url ) REFERENCES feed(url),
               |  FOREIGN KEY( stype ) REFERENCES subscription_type( stype )
               |)""".stripMargin
        object Mailable extends Creatable:       
          val Create =
            """|CREATE TABLE mailable(
               |  seqnum INTEGER,
               |  email VARCHAR(256),
               |  feed_url VARCHAR(1024),
               |  stype VARCHAR(32),
               |  within_type_id VARCHAR(1024),
               |  mailed BOOLEAN,
               |  PRIMARY KEY( seqnum ),
               |  FOREIGN KEY (feed_url, stype, within_type_id) REFERENCES assignable(feed_url, stype, within_type_id)
               |)""".stripMargin
      object Sequence:       
        object MailableSeq extends Creatable:       
          val Create = "CREATE SEQUENCE mailable_seq AS INTEGER"

