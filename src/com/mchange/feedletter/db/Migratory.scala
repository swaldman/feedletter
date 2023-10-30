package com.mchange.feedletter.db

import zio.*
import java.time.Instant
import javax.sql.DataSource
import com.mchange.feedletter.Config
import java.time.temporal.ChronoUnit

object Migratory:
  val DumpTimestampFormatter = java.time.format.DateTimeFormatter.ISO_INSTANT

  def prepareDumpFileForInstant( config : Config, instant : Instant ) : Task[os.Path] = ZIO.attemptBlocking:
    if !os.exists( config.dumpDir ) then os.makeDir.all( config.dumpDir )
    val ts = DumpTimestampFormatter.format( instant )
    config.dumpDir / ("feedletter-pg-dump." + ts + ".sql")

  private val DumpFileNameRegex = """^feedletter-pg-dump\.(.+)\.sql$""".r

  private def extractTimestampFromDumpFileName( dfn : String ) : Option[Instant] =
    DumpFileNameRegex.findFirstMatchIn(dfn)
      .map( m => m.group(1) )
      .map( DumpTimestampFormatter.parse )
      .map( Instant.from )

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
  def targetDbVersion : Int
  
  def dump(config : Config, ds : DataSource) : Task[Unit]
  def dbVersionStatus(config : Config, ds : DataSource) : Task[DbVersionStatus]
  def upMigrate(config : Config, ds : DataSource, from : Option[Int]) : Task[Unit]

  def migrate(config : Config, ds : DataSource) : Task[Unit] =
    def handleStatus( status : DbVersionStatus ) : Task[Unit] =
      status match
        case DbVersionStatus.Current(_) => ZIO.succeed( () )
        case DbVersionStatus.OutOfDate( schemaVersion, requiredVersion ) =>
          assert( schemaVersion < requiredVersion, s"An out-of-date scheme should have schema version (${schemaVersion}) < required version (${requiredVersion})" )
          upMigrate( config, ds, Some( schemaVersion ) )
        case DbVersionStatus.SchemaMetadataNotFound =>
          upMigrate( config, ds, None )
        case other =>
          throw new CannotUpMigrate( s"""${other}: ${other.errMessage.getOrElse("<no message>")}""" ) // we should never see <no message>
    for
      status <- dbVersionStatus(config, ds)
      _      <- handleStatus( status )
    yield ()
          

