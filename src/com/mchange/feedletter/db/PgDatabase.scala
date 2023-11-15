package com.mchange.feedletter.db

import zio.*
import java.sql.{Connection,Timestamp}
import javax.sql.DataSource
import com.mchange.feedletter.Config

import scala.util.Using
import java.sql.SQLException
import java.time.{Duration as JDuration,Instant,ZonedDateTime}

import com.mchange.sc.v1.log.*
import MLevel.*

import audiofluidity.rss.util.formatPubDate
import com.mchange.feedletter.{doDigestFeed, ItemContent, SubscriptionType}

object PgDatabase extends Migratory:
  private lazy given logger : MLogger = mlogger( this )

  val LatestSchema = PgSchema.V1
  override val targetDbVersion = LatestSchema.Version

  val DefaultMailBatchSize = 100
  val DefaultMailBatchDelaySeconds = 15 * 60 // 15 mins
  
  def fetchMetadataValue( conn : Connection, key : MetadataKey ) : Task[Option[String]] =
    ZIO.attemptBlocking( PgSchema.Unversioned.Table.Metadata.select( conn, key ) )

  override def dump(config : Config, ds : DataSource) : Task[os.Path] =
    def runDump( dumpFile : os.Path ) : Task[Unit] =
      ZIO.attemptBlocking:
        val parsedCommand = List("pg_dump", config.dbName)
        os.proc( parsedCommand ).call( stdout = dumpFile )
    for
      dumpFile <- Migratory.prepareDumpFileForInstant(config, java.time.Instant.now)
      _        <- runDump( dumpFile )
    yield dumpFile

  override def dbVersionStatus(config : Config, ds : DataSource) : Task[DbVersionStatus] =
    withConnection( ds ): conn =>
      val okeyDokeyIsh = 
        for
          mbDbVersion <- fetchMetadataValue(conn, MetadataKey.SchemaVersion)
          mbCreatorAppVersion <- fetchMetadataValue(conn, MetadataKey.CreatorAppVersion)
        yield
          try
            mbDbVersion.map( _.toInt ) match
              case Some( version ) if version == LatestSchema.Version => DbVersionStatus.Current( version )
              case Some( version ) if version < LatestSchema.Version => DbVersionStatus.OutOfDate( version, LatestSchema.Version )
              case Some( version ) => DbVersionStatus.UnexpectedVersion( Some(version.toString), mbCreatorAppVersion, Some( com.mchange.feedletter.BuildInfo.version ), Some(LatestSchema.Version.toString()) )
              case None => DbVersionStatus.SchemaMetadataDisordered( s"Expected key '${MetadataKey.SchemaVersion}' was not found in schema metadata!" )
          catch
            case nfe : NumberFormatException =>
              DbVersionStatus.UnexpectedVersion( mbDbVersion, mbCreatorAppVersion, Some( com.mchange.feedletter.BuildInfo.version ), Some(LatestSchema.Version.toString()) )
      okeyDokeyIsh.catchSome:
        case sqle : SQLException =>
          val dbmd = conn.getMetaData()
          try
            val rs = dbmd.getTables(null,null,PgSchema.Unversioned.Table.Metadata.Name,null)
            if !rs.next() then // the metadata table does not exist
              ZIO.succeed( DbVersionStatus.SchemaMetadataNotFound )
            else
              ZIO.succeed( DbVersionStatus.SchemaMetadataDisordered(s"Metadata table found, but an Exception occurred while accessing it: ${sqle.toString()}") )
          catch
            case t : SQLException =>
              WARNING.log("Exception while connecting to database.", t)
              ZIO.succeed( DbVersionStatus.ConnectionFailed )
  end dbVersionStatus
  
  override def upMigrate(config : Config, ds : DataSource, from : Option[Int]) : Task[Unit] =
    def upMigrateFrom_New() : Task[Unit] =
      TRACE.log( "upMigrateFrom_New()" )
      withConnection( ds ): conn =>
        ZIO.attemptBlocking:
          conn.setAutoCommit(false)
          PgSchema.Unversioned.Table.Metadata.create( conn )
          insertMetadataKeys(
            conn,
            (MetadataKey.SchemaVersion, "0"),
            (MetadataKey.CreatorAppVersion, com.mchange.feedletter.BuildInfo.version)
          )  
          conn.commit()
    
    def upMigrateFrom_0() : Task[Unit] =
      TRACE.log( "upMigrateFrom_0()" )
      withConnection( ds ): conn =>
        ZIO.attemptBlocking:
          conn.setAutoCommit(false)
          Using.resource( conn.createStatement() ): stmt =>
            PgSchema.V1.Table.Feed.create( stmt )
            PgSchema.V1.Table.Item.create( stmt )
            PgSchema.V1.Table.SubscriptionType.create( stmt )
            PgSchema.V1.Table.Assignable.create( stmt )
            PgSchema.V1.Table.Assignment.create( stmt )
            PgSchema.V1.Table.Subscription.create( stmt )
            PgSchema.V1.Table.Mailable.create( stmt )
            PgSchema.V1.Sequence.MailableSeq.create( stmt )
          insertMetadataKeys(
            conn,
            (MetadataKey.NextMailBatchTime, formatPubDate(ZonedDateTime.now)),
            (MetadataKey.MailBatchSize, DefaultMailBatchSize.toString()),
            (MetadataKey.MailBatchDelaySecs, DefaultMailBatchDelaySeconds.toString())
          )
          updateMetadataKeys(
            conn,
            (MetadataKey.SchemaVersion, "1"),
            (MetadataKey.CreatorAppVersion, com.mchange.feedletter.BuildInfo.version)
          )
          conn.commit()
          
    TRACE.log( s"upMigrate( from=${from} )" )
    from match
      case None      => upMigrateFrom_New()
      case Some( 0 ) => upMigrateFrom_0()
      case Some( `targetDbVersion` ) =>
        ZIO.fail( new CannotUpMigrate( s"Cannot upmigrate from current target DB version: V${targetDbVersion}" ) )
      case Some( other ) =>
        ZIO.fail( new CannotUpMigrate( s"Cannot upmigrate from unknown DB version: V${other}" ) )

  private def insertMetadataKeys( conn : Connection, pairs : (MetadataKey,String)* ) : Unit =
    pairs.foreach( ( mdkey, value ) => PgSchema.Unversioned.Table.Metadata.insert(conn, mdkey, value) )

  private def updateMetadataKeys( conn : Connection, pairs : (MetadataKey,String)* ) : Unit =
    pairs.foreach( ( mdkey, value ) => PgSchema.Unversioned.Table.Metadata.update(conn, mdkey, value) )

  private def ensureOpenAssignable( conn : Connection, feedUrl : String, stype : SubscriptionType, withinTypeId : String, forGuid : Option[String]) : Unit =
    LatestSchema.Table.Assignable.selectCompleted( conn, feedUrl, stype, withinTypeId) match
      case Some( false ) =>                                                               /* okay, ignore */
      case Some( true )  => throw new AssignableCompleted( feedUrl, stype, withinTypeId, forGuid ) /* uh oh! */
      case None =>
        LatestSchema.Table.Assignable.insert( conn, feedUrl, stype, withinTypeId, false )
  

  private def assignForSubscriptionType( conn : Connection, stype : SubscriptionType, feedUrl : String, guid : String, content : ItemContent, status : ItemStatus ) : Unit =
    stype.withinTypeId( feedUrl, guid, content, status ).foreach: wti =>
      ensureOpenAssignable( conn, feedUrl, stype, wti, Some(guid) )
      LatestSchema.Table.Assignment.insert( conn, feedUrl, stype, wti, guid )
      
  private def assign( conn : Connection, feedUrl : String, guid : String, content : ItemContent, status : ItemStatus ) : Unit =
    val subscriptionTypes = LatestSchema.Table.Subscription.selectSubscriptionTypeByFeedUrl( conn, feedUrl )
    subscriptionTypes.foreach( stype => assignForSubscriptionType( conn, stype, feedUrl, guid, content, status ) )
    LatestSchema.Table.Item.updateAssigned( conn, feedUrl, guid, true )
    
    
  // only unassigned items
  private def updateItem( config : Config, feed : Config.Feed, conn : Connection, feedUrl : String, guid : String, dbStatus : Option[ItemStatus], freshContent : ItemContent ) : Unit =
    dbStatus match
      case Some( prev @ ItemStatus( contentHash, lastChecked, stableSince ) ) =>
        val now = Instant.now
        val newContentHash = freshContent.##
        if newContentHash == contentHash then
          val newLastChecked = now
          LatestSchema.Table.Item.updateStable( conn, feedUrl, guid, newLastChecked )
          val stableSeconds = JDuration.between(stableSince, now).getSeconds()
          if stableSeconds > feed.awaitStabilizationSeconds then
            assign( conn, feedUrl, guid, freshContent, prev.copy( lastChecked = newLastChecked ) )
        else
          val newStableSince = now
          val newLastChecked = now
          LatestSchema.Table.Item.updateChanged( conn, feedUrl, guid, freshContent, ItemStatus( newContentHash, newLastChecked, newStableSince ) )
      case None =>
        LatestSchema.Table.Item.insertNew(conn, feedUrl, guid, freshContent)
        

  def updateFeed( config : Config, feed : Config.Feed, ds : DataSource, feedUrl : String ) : Task[Unit] =
    withConnection( ds ): conn =>
      ZIO.attemptBlocking:
        val feedDigest = doDigestFeed( feedUrl )
        ???
