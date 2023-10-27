package com.mchange.feedletter.db

object PgSchema:

  trait TraitV0:
    val TABLE_METADATA_CREATE =
      "CREATE TABLE metadata( key VARCHAR(64) PRIMARY KEY, value VARCHAR(64) NOT NULL )"
    val TABLE_METADATA_INSERT =
      "INSERT INTO metadata VALUES( ?, ? )"
    val TABLE_METADATA_SELECT =
      "SELECT value FROM metadata WHERE key = ?"
      
  trait TraitV1 extends TraitV0:
    val TABLE_FEED_CREATE =
      "CREATE TABLE feed( url VARCHAR(1024) PRIMARY KEY )"
    val TABLE_ITEM_CREATE =
      """|CREATE TABLE item(
         |  feed_url VARCHAR(1024),
         |  guid VARCHAR(1024),
         |  contentHash INTEGER,
         |  publicationDate TIMESTAMP,
         |  lastChecked TIMESTAMP,
         |  stableSince TIMESTAMP,
         |  assigned BOOLEAN,
         |  PRIMARY KEY(feed_url, guid),
         |  FOREIGN KEY(feed_url) REFERENCES feed(url)
         |)""".stripMargin
    val TABLE_SUBSCRIPTION_TYPE_CREATE =
      "CREATE TABLE subscription_type( stype VARCHAR(32) )"
    val TABLE_ASSIGNABLE_CREATE = // an assignable represents a collection of posts for a single mail
      """|CREATE TABLE assignable(
         |  feed_url VARCHAR(1024),
         |  stype VARCHAR(32),
         |  within_type_id VARCHAR(1024),
         |  completed : BOOLEAN,
         |  PRIMARY KEY(feed_url, stype, within_type_id),
         |  FOREIGN KEY(feed_url) REFERENCES feed(url),
         |  FOREIGN KEY stype REFERENCES subscription_type(stype)
         |)""".stripMargin
    val TABLE_ASSIGNMENT_CREATE = // an assignment represents a membership of a post in a collection
      """|CREATE TABLE assignment(
         |  feed_url VARCHAR(1024),
         |  stype VARCHAR(32),
         |  within_type_id VARCHAR(1024),
         |  guid VARCHAR(1024),
         |  FOREIGN KEY( guid ) REFERENCES item( guid ),
         |  FOREIGN KEY( feed_url, stype, within_type_id ) REFERENCES assignable( feed_url, stype, within_type_id )
         |)""".stripMargin
    val TABLE_SUBSCRIPTION_CREATE =
      """|CREATE TABLE subscription(
         |  email VARCHAR(256),
         |  feed_url VARCHAR(256),
         |  stype VARCHAR(32),
         |  PRIMARY KEY( email, feed_url ),
         |  FOREIGN KEY( feed_url ) REFERENCES feed(url)
         |  FOREIGN KEY( stype ) REFERENCES subscription_type( stype )
         |)""".stripMargin
    val SEQUENCE_MAILABLE_SEQ_CREATE =
      "CREATE SEQUENCE mailable_seq AS INTEGER"
    val TABLE_MAILABLE_CREATE =
      """|CREATE TABLE mailable(
         |  seqnum INTEGER,
         |  email VARCHAR(256),
         |  feed_url VARCHAR(1024),
         |  stype VARCHAR(32),
         |  within_type_id VARCHAR(1024),
         |  mailed : BOOLEAN,
         |  PRIMARY KEY( seqnum ),
         |  FOREIGN KEY (feed_url, stype, within_type_id) REFERENCES assignable(feed_url, stype, within_type_id)
         |)""".stripMargin

  object V0 extends TraitV0
  object V1 extends TraitV1
