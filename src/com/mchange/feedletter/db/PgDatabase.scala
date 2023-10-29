package com.mchange.feedletter.db

import zio.*
import java.sql.Connection
import javax.sql.DataSource
import com.mchange.feedletter.Config

import scala.util.Using
import java.sql.SQLException
import java.time.ZonedDateTime

import audiofluidity.rss.util.formatPubDate

object PgDatabase extends Migratory:
  val LatestSchema = PgSchema.V1
  override val targetDbVersion = LatestSchema.Version

  val DefaultMailBatchSize = 100
  val DefaultMailBatchDelaySeconds = 15 * 60 // 15 mins
  
  enum MetadataKey:
    case SchemaVersion
    case NextMailBatchTime
    case MailBatchSize
    case MailBatchDelaySecs

  enum SubscriptionType:
    case Immediate
    case Weekly

  val DumpTimestampFormatter = java.time.format.DateTimeFormatter.ISO_INSTANT

  override def dump(config : Config, ds : DataSource) : Task[Unit] = ZIO.attemptBlocking:
    if !os.exists( config.dumpDir ) then os.makeDir.all( config.dumpDir )
    val parsedCommand = List("pg_dump", config.dbName)
    val ts = DumpTimestampFormatter.format( java.time.Instant.now )
    val dumpFile = config.dumpDir / ("feedletter-pg-dump." + ts + ".sql")
    os.proc( parsedCommand ).call( stdout = dumpFile )
  
  override def discoveredDbVersion(config : Config, ds : DataSource) : Task[Option[Int]] =
    def extractVersion( conn : Connection ) : Task[Int] = ZIO.attemptBlocking:
      Using.resource( conn.prepareStatement( LatestSchema.TABLE_METADATA_SELECT ) ): ps =>
        ps.setString(1, MetadataKey.SchemaVersion.toString)
        Using.resource( ps.executeQuery() ): rs =>
          if !rs.next() then throw new UnexpectedlyEmptyResultSet("Expected a value for key 'SchemaVersion', none found.") else
            val out = rs.getString(1).toInt
            if rs.next() then throw new NonUniqueRow("Expected a unique value for metadata key 'SchemaVersion'. Multiple rows found.") else
              out
    end extractVersion
    
    withConnection( ds ): conn =>
      extractVersion(conn).map( i => Some(i) ).catchSome:
        case sqle : SQLException =>
          val dbmd = conn.getMetaData()
          val rs = dbmd.getTables(null,null,LatestSchema.TABLE_METADATA_NAME,null)
          if !rs.next() then // the metadata table does not exist
            ZIO.succeed( None )
          else
            ZIO.fail( sqle )
  end discoveredDbVersion
  
  override def upMigrate(config : Config, ds : DataSource, from : Option[Int]) : Task[Unit] =
    def upMigrateFrom_New() : Task[Unit] =
      withConnection( ds ): conn =>
        ZIO.attemptBlocking:
          conn.setAutoCommit(false)
          Using.resource( conn.createStatement() ): stmt =>
            stmt.executeUpdate( PgSchema.V0.TABLE_METADATA_CREATE )
          insertMetadataKeys( PgSchema.V0, conn, (MetadataKey.SchemaVersion, "0") )  
          conn.commit()
    
    def upMigrateFrom_0() : Task[Unit] =
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
          updateMetadataKeys(PgSchema.V1, conn, (MetadataKey.SchemaVersion, "1"))
          conn.commit()
          
    from match
      case None      => upMigrateFrom_New()
      case Some( 0 ) => upMigrateFrom_0()
      case Some( targetDbVersion ) =>
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

   
