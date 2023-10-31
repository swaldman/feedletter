package com.mchange.feedletter.db

import zio.*
import java.sql.Connection
import javax.sql.DataSource
import com.mchange.feedletter.Config

import scala.util.Using
import java.sql.SQLException
import java.time.ZonedDateTime

import com.mchange.sc.v1.log.*
import MLevel.*

import audiofluidity.rss.util.formatPubDate

object PgDatabase extends Migratory:
  private lazy given logger : MLogger = mlogger( this )

  val LatestSchema = PgSchema.V1
  override val targetDbVersion = LatestSchema.Version

  val DefaultMailBatchSize = 100
  val DefaultMailBatchDelaySeconds = 15 * 60 // 15 mins
  
  enum MetadataKey:
    case SchemaVersion
    case CreatorAppVersion
    case NextMailBatchTime
    case MailBatchSize
    case MailBatchDelaySecs

  enum SubscriptionType:
    case Immediate
    case Weekly

  def fetchMetadataValue( conn : Connection, key : MetadataKey ) : Task[Option[String]] = ZIO.attemptBlocking:
    Using.resource( conn.prepareStatement( LatestSchema.TABLE_METADATA_SELECT ) ): ps =>
      ps.setString(1, MetadataKey.SchemaVersion.toString)
      Using.resource( ps.executeQuery() ): rs =>
        zeroOrOneResult( s"metadata key '$key'", rs )( _.getString(1) )

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
            val rs = dbmd.getTables(null,null,LatestSchema.TABLE_METADATA_NAME,null)
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
          Using.resource( conn.createStatement() ): stmt =>
            stmt.executeUpdate( PgSchema.V0.TABLE_METADATA_CREATE )
          insertMetadataKeys(
            PgSchema.V0,
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
            stmt.executeUpdate( PgSchema.V1.TABLE_FEED_CREATE )
            stmt.executeUpdate( PgSchema.V1.TABLE_ITEM_CREATE )
            stmt.executeUpdate( PgSchema.V1.TABLE_SUBSCRIPTION_TYPE_CREATE )
            stmt.executeUpdate( PgSchema.V1.TABLE_ASSIGNABLE_CREATE )
            stmt.executeUpdate( PgSchema.V1.TABLE_ASSIGNMENT_CREATE )
            stmt.executeUpdate( PgSchema.V1.TABLE_SUBSCRIPTION_CREATE )
            stmt.executeUpdate( PgSchema.V1.SEQUENCE_MAILABLE_SEQ_CREATE )
            stmt.executeUpdate( PgSchema.V1.TABLE_MAILABLE_CREATE )
          Using.resource( conn.prepareStatement( PgSchema.V1.TABLE_SUBSCRIPTION_TYPE_INSERT ) ): ps =>
            ps.setString( 1, SubscriptionType.Immediate.toString() )
            ps.executeUpdate()
            ps.setString( 1, SubscriptionType.Weekly.toString() )
            ps.executeUpdate()
          insertMetadataKeys(
            PgSchema.V1,
            conn,
            (MetadataKey.NextMailBatchTime, formatPubDate(ZonedDateTime.now)),
            (MetadataKey.MailBatchSize, DefaultMailBatchSize.toString()),
            (MetadataKey.MailBatchDelaySecs, DefaultMailBatchDelaySeconds.toString())
          )
          updateMetadataKeys(
            PgSchema.V1,
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

  private def insertMetadataKeys( schema : PgSchema.Base, conn : Connection, pairs : (MetadataKey,String)* ) : Unit =
    Using.resource( conn.prepareStatement(schema.TABLE_METADATA_INSERT) ): ps =>
      pairs.foreach: ( mdkey, value ) =>
        ps.setString(1, mdkey.toString())
        ps.setString(2, value)
        ps.executeUpdate()

  private def updateMetadataKeys( schema : PgSchema.Base, conn : Connection, pairs : (MetadataKey,String)* ) : Unit =
    Using.resource( conn.prepareStatement(schema.TABLE_METADATA_UPDATE) ): ps =>
      pairs.foreach: ( mdkey, value ) =>
        ps.setString(1, value)
        ps.setString(2, mdkey.toString())
        ps.executeUpdate()

   
