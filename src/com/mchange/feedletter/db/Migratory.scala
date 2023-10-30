package com.mchange.feedletter.db

import zio.*
import java.time.Instant
import javax.sql.DataSource
import com.mchange.feedletter.Config
import java.time.temporal.ChronoUnit
import com.mchange.feedletter.Main.config
import com.mchange.feedletter.db.Migratory.lastHourDumpFileExists
import com.mchange.feedletter.BuildInfo.version
import com.mchange.sc.v1.log.*
import MLevel.*

object Migratory:
  private val DumpTimestampFormatter = java.time.format.DateTimeFormatter.ISO_INSTANT

  private val DumpFileNameRegex = """^feedletter-pg-dump\.(.+)\.sql$""".r

  private def dumpFileName( timestamp : String ) : String =
    "feedletter-pg-dump." + timestamp + ".sql"
    
  private def extractTimestampFromDumpFileName( dfn : String ) : Option[Instant] =
    DumpFileNameRegex.findFirstMatchIn(dfn)
      .map( m => m.group(1) )
      .map( DumpTimestampFormatter.parse )
      .map( Instant.from )

  def prepareDumpFileForInstant( config : Config, instant : Instant ) : Task[os.Path] = ZIO.attemptBlocking:
    if !os.exists( config.dumpDir ) then os.makeDir.all( config.dumpDir )
    val ts = DumpTimestampFormatter.format( instant )
    config.dumpDir / dumpFileName(ts)

  def lastHourDumpFileExists( config : Config ) : Task[Boolean] = ZIO.attemptBlocking:
    if os.exists( config.dumpDir ) then
      val instants =
        os.list( config.dumpDir )
          .map( _.last )
          .map( extractTimestampFromDumpFileName )
          .collect { case Some(instant) => instant }
      val now = Instant.now
      val anHourAgo = now.minus(1, ChronoUnit.HOURS)
      instants.exists( i => anHourAgo.compareTo(i) < 0 )
    else
      false
    
trait Migratory:
  private lazy given logger : MLogger = mlogger( this )

  def targetDbVersion : Int
  
  def dump(config : Config, ds : DataSource) : Task[Unit]
  def dbVersionStatus(config : Config, ds : DataSource) : Task[DbVersionStatus]
  def upMigrate(config : Config, ds : DataSource, from : Option[Int]) : Task[Unit]

  def migrate(config : Config, ds : DataSource) : Task[Unit] =
    def handleStatus( status : DbVersionStatus ) : Task[Unit] =
      TRACE.log( s"handleStatus( ${status} )" )
      status match
        case DbVersionStatus.Current(version) =>
          INFO.log( s"Schema up-to-date (current version: ${version})" )
          ZIO.succeed( () )
        case DbVersionStatus.OutOfDate( schemaVersion, requiredVersion ) =>
          assert( schemaVersion < requiredVersion, s"An out-of-date scheme should have schema version (${schemaVersion}) < required version (${requiredVersion})" )
          INFO.log( s"Up-migrating from schema version ${schemaVersion})" )
          upMigrate( config, ds, Some( schemaVersion ) ) *> migrate( config, ds )
        case DbVersionStatus.SchemaMetadataNotFound => // uninitialized db, we presume
          INFO.log( s"Initializing new schema")
          upMigrate( config, ds, None ) *> migrate( config, ds )
        case other =>
          throw new CannotUpMigrate( s"""${other}: ${other.errMessage.getOrElse("<no message>")}""" ) // we should never see <no message>
    for
      status <- dbVersionStatus(config, ds)
      _      <- handleStatus( status )
    yield ()
          
  def cautiousMigrate( config : Config, ds : DataSource ) : Task[Unit] =
    val safeToTry = 
      for
        initialStatus <- dbVersionStatus(config, ds)
        dumped <- lastHourDumpFileExists(config)
      yield
        initialStatus == DbVersionStatus.SchemaMetadataNotFound || dumped

    safeToTry.flatMap: safe =>
      if safe then
        migrate(config, ds)
      else
        ZIO.fail( new NoRecentDump("Please dump the database prior to migrating, no recent dump file found.") )
      
