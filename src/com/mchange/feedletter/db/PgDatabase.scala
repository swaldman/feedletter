package com.mchange.feedletter.db

import zio.*
import java.sql.{Connection,Timestamp}
import javax.sql.DataSource

import scala.collection.{immutable,mutable}
import scala.util.Using
import scala.util.control.NonFatal

import scala.math.Ordering.Implicits.infixOrderingOps

import java.sql.SQLException
import java.time.{Duration as JDuration,Instant,ZonedDateTime,ZoneId}
import java.time.temporal.ChronoUnit

import com.mchange.feedletter.*
import MLevel.*

import audiofluidity.rss.util.formatPubDate

import com.mchange.mailutil.*
import com.mchange.cryptoutil.{*, given}

import com.mchange.feedletter.BuildInfo
import com.mchange.feedletter.api.ApiLinkGenerator
import com.mchange.feedletter.Main.Admin.subscribe
import cats.instances.try_

object PgDatabase extends Migratory, SelfLogging:
  val LatestSchema = PgSchema.V1
  override val targetDbVersion = LatestSchema.Version

  private def fetchMetadataValue( conn : Connection, key : MetadataKey ) : Option[String] =
    PgSchema.Unversioned.Table.Metadata.select( conn, key )

  private def zfetchMetadataValue( conn : Connection, key : MetadataKey ) : Task[Option[String]] =
    ZIO.attemptBlocking( PgSchema.Unversioned.Table.Metadata.select( conn, key ) )

  private def fetchDbName(conn : Connection) : Task[String] =
    ZIO.attemptBlocking:
      Using.resource(conn.createStatement()): stmt =>
        Using.resource(stmt.executeQuery("SELECT current_database()")): rs =>
          uniqueResult("select-current-database-name", rs)( _.getString(1) )

  private def fetchDumpDir(conn : Connection) : Task[os.Path] =
    for
      mbDumpDir <- Config.zfetchValue(conn, ConfigKey.DumpDbDir)
    yield
      os.Path( mbDumpDir.getOrElse( throw new ConfigurationMissing(ConfigKey.DumpDbDir) ) )

  override def fetchDumpDir( ds : DataSource ) : Task[os.Path] =
    withConnectionZIO(ds)( fetchDumpDir )

  override def dump(ds : DataSource) : Task[os.Path] =
    def runDump( dbName : String, dumpFile : os.Path ) : Task[Unit] =
      ZIO.attemptBlocking:
        val parsedCommand = List("pg_dump", dbName)
        os.proc( parsedCommand ).call( stdout = dumpFile )
    withConnectionZIO( ds ): conn =>
      for
        dbName   <- fetchDbName(conn)
        dumpDir  <- fetchDumpDir(conn)
        dumpFile <- Migratory.prepareDumpFileForInstant(dumpDir, java.time.Instant.now)
        _        <- runDump( dbName, dumpFile )
      yield dumpFile

  override def dbVersionStatus(ds : DataSource) : Task[DbVersionStatus] =
    withConnectionZIO( ds ): conn =>
      val okeyDokeyIsh =
        for
          mbDbVersion <- zfetchMetadataValue(conn, MetadataKey.SchemaVersion)
          mbCreatorAppVersion <- zfetchMetadataValue(conn, MetadataKey.CreatorAppVersion)
        yield
          try
            mbDbVersion.map( _.toInt ) match
              case Some( version ) if version == LatestSchema.Version => DbVersionStatus.Current( version )
              case Some( version ) if version < LatestSchema.Version => DbVersionStatus.OutOfDate( version, LatestSchema.Version )
              case Some( version ) => DbVersionStatus.UnexpectedVersion( Some(version.toString), mbCreatorAppVersion, Some( BuildInfo.version ), Some(LatestSchema.Version.toString()) )
              case None => DbVersionStatus.SchemaMetadataDisordered( s"Expected key '${MetadataKey.SchemaVersion}' was not found in schema metadata!" )
          catch
            case nfe : NumberFormatException =>
              DbVersionStatus.UnexpectedVersion( mbDbVersion, mbCreatorAppVersion, Some( BuildInfo.version ), Some(LatestSchema.Version.toString()) )
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

  override def upMigrate(ds : DataSource, from : Option[Int]) : Task[Unit] =
    def upMigrateFrom_New() : Task[Unit] =
      TRACE.log( "upMigrateFrom_New()" )
      withConnectionTransactional( ds ): conn =>
        PgSchema.Unversioned.Table.Metadata.create( conn )
        insertMetadataKeys(
          conn,
          (MetadataKey.SchemaVersion, "0"),
          (MetadataKey.CreatorAppVersion, BuildInfo.version)
        )

    def upMigrateFrom_0() : Task[Unit] =
      TRACE.log( "upMigrateFrom_0()" )
      withConnectionTransactional( ds ): conn =>
        Using.resource( conn.createStatement() ): stmt =>
          PgSchema.V1.Table.Config.create( stmt )
          PgSchema.V1.Table.Flags.create( stmt )
          PgSchema.V1.Table.Feed.create( stmt )
          PgSchema.V1.Table.Feed.Sequence.FeedSeq.create( stmt )
          PgSchema.V1.Table.Item.Type.ItemAssignability.create( stmt )
          PgSchema.V1.Table.Item.create( stmt )
          PgSchema.V1.Table.Item.Index.ItemAssignability.create( stmt )
          PgSchema.V1.Table.Subscribable.create( stmt )
          PgSchema.V1.Table.Assignable.create( stmt )
          PgSchema.V1.Table.Assignment.create( stmt )
          PgSchema.V1.Table.Subscription.create( stmt )
          PgSchema.V1.Table.Subscription.Sequence.SubscriptionSeq.create( stmt )
          PgSchema.V1.Table.Subscription.Index.SubscriptionIdConfirmed.create( stmt )
          PgSchema.V1.Table.Subscription.Index.DestinationUniqueSubscribableName.create( stmt )
          PgSchema.V1.Table.MailableTemplate.create( stmt )
          PgSchema.V1.Table.Mailable.create( stmt )
          PgSchema.V1.Table.Mailable.Sequence.MailableSeq.create( stmt )
          PgSchema.V1.Table.MastoPostable.create( stmt )
          PgSchema.V1.Table.MastoPostable.Sequence.MastoPostableSeq.create( stmt )
          PgSchema.V1.Table.MastoPostableMedia.create( stmt )
        updateMetadataKeys(
          conn,
          (MetadataKey.SchemaVersion, "1"),
          (MetadataKey.CreatorAppVersion, BuildInfo.version)
        )

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

  private def insertConfigKeys( conn : Connection, pairs : (ConfigKey,String)* ) : Unit =
    pairs.foreach( ( cfgkey, value ) => LatestSchema.Table.Config.insert(conn, cfgkey, value) )
    setMustReloadDaemon(conn)

  private def updateConfigKeys( conn : Connection, pairs : (ConfigKey,String)* ) : Unit =
    pairs.foreach( ( cfgkey, value ) => LatestSchema.Table.Config.update(conn, cfgkey, value) )
    setMustReloadDaemon(conn)

  private def upsertConfigKeys( conn : Connection, pairs : (ConfigKey,String)* ) : Unit =
    pairs.foreach( ( cfgkey, value ) => LatestSchema.Table.Config.upsert(conn, cfgkey, value) )
    setMustReloadDaemon(conn)

  private def sort( tups : Set[Tuple2[ConfigKey,String]] ) : immutable.SortedSet[Tuple2[ConfigKey,String]] =
    immutable.SortedSet.from( tups )( using Ordering.by( tup => (tup(0).toString().toUpperCase, tup(1) ) ) )

  private def upsertConfigKeyMapAndReport( conn : Connection, map : Map[ConfigKey,String] ) : immutable.SortedSet[Tuple2[ConfigKey,String]] =
    upsertConfigKeys( conn, map.toList* )
    sort( LatestSchema.Table.Config.selectTuples(conn) )

  def upsertConfigKeyMapAndReport( ds : DataSource, map : Map[ConfigKey,String] ) : Task[immutable.SortedSet[Tuple2[ConfigKey,String]]] =
    withConnectionTransactional( ds )( conn => upsertConfigKeyMapAndReport( conn, map ) )

  def reportConfigKeys( ds : DataSource ): Task[immutable.SortedSet[Tuple2[ConfigKey,String]]] =
    withConnectionTransactional( ds )( conn => sort( LatestSchema.Table.Config.selectTuples( conn ) ) )

  /*
  private def lastCompletedAssignableWithinTypeStatus( conn : Connection, feedId : FeedId, subscribableName : SubscribableName ) : Option[AssignableWithinTypeStatus] =
    val withinTypeId = LatestSchema.Table.Assignable.selectWithinTypeIdLastCompleted( conn, subscribableName )
    withinTypeId.map: wti =>
      val count = LatestSchema.Table.Assignment.selectCountWithinAssignable( conn, subscribableName, wti )
      AssignableWithinTypeStatus( wti, count )

  private def mostRecentOpenAssignableWithinTypeStatus( conn : Connection, feedId : FeedId, subscribableName : SubscribableName ) : Option[AssignableWithinTypeStatus] =
    val withinTypeId = LatestSchema.Table.Assignable.selectWithinTypeIdMostRecentOpen( conn, subscribableName )
    withinTypeId.map: wti =>
      val count = LatestSchema.Table.Assignment.selectCountWithinAssignable( conn, subscribableName, wti )
      AssignableWithinTypeStatus( wti, count )
  */

  private def ensureOpenAssignable( conn : Connection, feedId : FeedId, subscribableName : SubscribableName, withinTypeId : String, forGuid : Option[Guid]) : Unit =
    LatestSchema.Table.Assignable.selectOpened( conn, subscribableName, withinTypeId ) match
      case Some( time ) => /* okay, ignore */
      case None => LatestSchema.Table.Assignable.insert( conn, subscribableName, withinTypeId, Instant.now )

  def mostRecentlyOpenedAssignableWithinTypeStatus( conn : Connection, subscribableName : SubscribableName ) : Option[AssignableWithinTypeStatus] =
    val withinTypeId = LatestSchema.Table.Assignable.selectWithinTypeIdMostRecentOpened( conn, subscribableName )
    withinTypeId.map: wti =>
      val count = LatestSchema.Table.Assignment.selectCountWithinAssignable( conn, subscribableName, wti )
      AssignableWithinTypeStatus( wti, count )

  def lastCompletedWithinTypeId( conn : Connection, subscribableName : SubscribableName ) : Option[String] =
    LatestSchema.Table.Subscribable.selectLastCompletedWti( conn, subscribableName )

  private def assignForSubscribable( conn : Connection, subscribableName : SubscribableName, feedId : FeedId, guid : Guid, content : ItemContent, status : ItemStatus ) : Unit =
    TRACE.log( s"assignForSubscriptionManager( $conn, $subscribableName, $feedId, $guid, $content, $status )" )
    val subscriptionManager = LatestSchema.Table.Subscribable.selectManager( conn, subscribableName )
    subscriptionManager.withinTypeId( conn, subscribableName, feedId, guid, content, status ).foreach: wti =>
      ensureOpenAssignable( conn, feedId, subscribableName, wti, Some(guid) )
      LatestSchema.Table.Assignment.insert( conn, subscribableName, wti, guid )
      DEBUG.log( s"Item with GUID '${guid}' from feed with ID ${feedId} has been assigned in subscribable '${subscribableName}'." )

  private def assign( conn : Connection, feedId : FeedId, guid : Guid, content : ItemContent, status : ItemStatus ) : Unit =
    TRACE.log( s"assign( $conn, $feedId, $guid, $content, $status )" )
    val subscribableNames = LatestSchema.Table.Subscribable.selectSubscribableNamesByFeedId( conn, feedId )
    subscribableNames.foreach( subscribableName => assignForSubscribable( conn, subscribableName, feedId, guid, content, status ) )
    LatestSchema.Table.Item.updateLastCheckedAssignability( conn, feedId, guid, status.lastChecked, ItemAssignability.Assigned )
    DEBUG.log( s"Item with GUID '${guid}' from feed with ID ${feedId} has been assigned in all subscribables to that feed." )

  private def updateAssignItem( conn : Connection, fi : FeedInfo, guid : Guid, dbStatus : Option[ItemStatus], freshContent : ItemContent, now : Instant ) : Unit =
    TRACE.log( s"updateAssignItem( $conn, $fi, $guid, $dbStatus, $freshContent, $now )" )
    dbStatus match
      case Some( prev @ ItemStatus( contentHash, firstSeen, lastChecked, stableSince, ItemAssignability.Unassigned ) ) =>
        val seenMinutes = JDuration.between(firstSeen, now).get( ChronoUnit.SECONDS ) / 60     // ChronoUnite.Minutes fails, "UnsupportedTemporalTypeException: Unsupported unit: Minutes"
        val stableMinutes = JDuration.between(stableSince, now).get( ChronoUnit.SECONDS ) / 60 // ChronoUnite.Minutes fails, "UnsupportedTemporalTypeException: Unsupported unit: Minutes"
        def afterMinDelay = seenMinutes > fi.minDelayMinutes
        def sufficientlyStable = stableMinutes > fi.awaitStabilizationMinutes
        def pastMaxDelay = seenMinutes > fi.maxDelayMinutes
        val newContentHash = freshContent.contentHash
        /* don't update empty over actual content, treat empty content (rare, since guid would still have been found) as just stability */
        if newContentHash == contentHash then
          val newLastChecked = now
          LatestSchema.Table.Item.updateStable( conn, fi.feedId, guid, newLastChecked )
          DEBUG.log( s"Updated last checked time on stable unassigned item, feed ID ${fi.feedId}, guid '${guid}'." )
          if (afterMinDelay && sufficientlyStable) || pastMaxDelay then
            assign( conn, fi.feedId, guid, freshContent, prev.copy( lastChecked = newLastChecked ) )
        else
          val newStableSince = now
          val newLastChecked = now
          LatestSchema.Table.Item.updateChanged( conn, fi.feedId, guid, freshContent, ItemStatus( newContentHash, firstSeen, newLastChecked, newStableSince, ItemAssignability.Unassigned ) )
          DEBUG.log( s"Updated stable since and last checked times, and content cache, on modified unassigned item, feed ID ${fi.feedId}, guid '${guid}'." )
          if pastMaxDelay then
            assign( conn, fi.feedId, guid, freshContent, prev.copy( lastChecked = newLastChecked ) )
      case Some( prev @ ItemStatus( contentHash, firstSeen, lastChecked, stableSince, ItemAssignability.Assigned ) ) =>
        val newContentHash = freshContent.contentHash
        if newContentHash != contentHash then
          val newStatus = ItemStatus( newContentHash, firstSeen, now, now, ItemAssignability.Assigned )
          LatestSchema.Table.Item.updateChanged( conn, fi.feedId, guid, freshContent, newStatus )
          DEBUG.log( s"Updated an already-assigned, not yet cleared, item which has been modified, feed ID ${fi.feedId}, guid '${guid}'." )
        else
          LatestSchema.Table.Item.updateStable( conn, fi.feedId, guid, now )
          DEBUG.log( s"Updated last-checked-time on a stable already-assigned, not yet cleared, item, feed ID ${fi.feedId}, guid '${guid}'." )
      case Some( ItemStatus( _, _, _, _, ItemAssignability.Cleared ) )  => /* ignore, we're done with this one */
      case Some( ItemStatus( _, _, _, _, ItemAssignability.Excluded ) ) => /* ignore, we don't assign  */
      case None =>
        def doUnassignedInsert() =
          LatestSchema.Table.Item.insertNew(conn, fi.feedId, guid, Some(freshContent), ItemAssignability.Unassigned)
          DEBUG.log( s"Added new item, feed ID ${fi.feedId}, guid '${guid}'." )
          val dbStatus = LatestSchema.Table.Item.checkStatus( conn, fi.feedId, guid ).getOrElse:
            throw new AssertionError("Just inserted row is not found???")
          if fi.minDelayMinutes <= 0 && fi.awaitStabilizationMinutes <= 0 then // if eligible for immediate assignment...
            assign( conn, fi.feedId, guid, freshContent, dbStatus )
        def doExcludingInsert( pd : Instant ) =
          WARNING.log(s"Excluding item found with parseable publication date '${pd}', which is prior to time of initial subscription, feed ID ${fi.feedId}, guid '${guid}'." )
          LatestSchema.Table.Item.insertNew( conn, fi.feedId, guid, None, ItemAssignability.Excluded ) // don't cache items we're excluding anyway
        freshContent.pubDate match
          case Some( pd ) =>
            if pd > fi.added then
              doUnassignedInsert() // skip items known to be published prior to subscription
            else
              doExcludingInsert( pd )
          case None =>
            doUnassignedInsert()

  private def updateAssignItems( conn : Connection, fi : FeedInfo ) : Unit =
    val nextAssign = fi.lastAssigned.plusSeconds( fi.assignEveryMinutes * 60 )
    val now = Instant.now()
    if now > nextAssign then
      val FeedDigest( fileOrderedGuids, guidToItemContent, timestamp ) = FeedDigest( fi.feedUrl, now )
      // we feed the entries in reverse of their reverse-chronological file ordering!
      fileOrderedGuids.reverse.map( guid => (guid, guidToItemContent( guid ) ) ).foreach: ( guid, freshContent ) =>
        val dbStatus = LatestSchema.Table.Item.checkStatus( conn, fi.feedId, guid )
        updateAssignItem( conn, fi, guid, dbStatus, freshContent, timestamp )
      DEBUG.log( s"Deleting any as-yet-unassigned items that have been deleted from feed with ID ${fi.feedId}" )
      val deleted = LatestSchema.Table.Item.deleteDisappearedUnassignedForFeed( conn, fi.feedId, guidToItemContent.keySet ) // so that if a post is deleted before it has been assigned, it won't be notified
      if deleted > 0 then
        INFO.log( s"Deleted ${deleted} disappeared unassigned items from feed with ID ${fi.feedId}." )
        DEBUG.log( s"GUIDs in feed with ID ${fi.feedId} that should have been retained: " + guidToItemContent.keySet.mkString(", ") )
      LatestSchema.Table.Feed.updateLastAssigned(conn, fi.feedId, timestamp)
      INFO.log( s"Updated/assigned all items from feed with ID ${fi.feedId}, feed URL '${fi.feedUrl}'" )

  // it's probably fine to use cached values, because we recache them continually, even after assignment
  // so they should never be very stale.
  private def materializeAssignable( conn : Connection, assignableKey : AssignableKey ) : Set[ItemContent] =
    LatestSchema.Join.ItemAssignment.selectItemContentsForAssignable( conn, assignableKey.subscribableName, assignableKey.withinTypeId )

  private def route( conn : Connection, assignableKey : AssignableKey, apiLinkGenerator : ApiLinkGenerator ) : Unit =
    val subscriptionManager = LatestSchema.Table.Subscribable.selectManager( conn, assignableKey.subscribableName )
    route( conn, assignableKey, subscriptionManager, apiLinkGenerator )

  private def route( conn : Connection, assignableKey : AssignableKey, sman : SubscriptionManager, apiLinkGenerator : ApiLinkGenerator ) : Unit =
    val AssignableKey( subscribableName, withinTypeId ) = assignableKey
    val contents = materializeAssignable(conn, assignableKey)
    val idestinations = LatestSchema.Table.Subscription.selectConfirmedIdentifiedDestinationsForSubscribable( conn, subscribableName )
    val narrowed =
      val eithers = idestinations.map( sman.narrowIdentifiedDestination )
      val tmp = Set.newBuilder[IdentifiedDestination[sman.D]]
      eithers.foreach: either =>
        either match
          case Left( badIdDestination ) =>
            WARNING.log( s"Destination '${badIdDestination.destination.unique}' is unsuitable for subscription '${assignableKey.subscribableName}', and will be ignored." )
          case Right( goodIdDestination ) =>
            tmp += goodIdDestination
      tmp.result()
    sman.route(conn, assignableKey, contents, narrowed, apiLinkGenerator)

  def queueForMailing( conn : Connection, contents : String, from : AddressHeader[From], replyTo : Option[AddressHeader[ReplyTo]], to : AddressHeader[To], templateParams : TemplateParams, subject : String ) : Unit =
    queueForMailing( conn, contents, from, replyTo, Set(Tuple2(to,templateParams)), subject )

  def queueForMailing( conn : Connection, contents : String, from : AddressHeader[From], replyTo : Option[AddressHeader[ReplyTo]], tosWithParams : Set[(AddressHeader[To],TemplateParams)], subject : String ) : Unit = 
    val hash = Hash.SHA3_256.hash( contents.getBytes( scala.io.Codec.UTF8.charSet ) )
    LatestSchema.Table.MailableTemplate.ensure( conn, hash, contents )
    LatestSchema.Table.Mailable.insertBatch( conn, hash, from, replyTo, tosWithParams, subject, 0 )

  private def resilientDelayedForFeedInfo( ds : DataSource, fi : FeedInfo ) : Task[Unit] =
    val simple = withConnectionTransactional( ds )( conn => updateAssignItems( conn, fi ) ).zlogError( DEBUG, what = "Attempt to update/assign for ${fi}" )
    val retrying = simple.retry( Schedule.exponential( 10.seconds, 1.5f ) && Schedule.upTo( 3.minutes ) ) // XXX: hard-coded
    val delaying =
      for
        _ <- ZIO.sleep( math.round(math.random * 20).seconds ) // XXX: hard-coded
        _ <- retrying
      yield()
    delaying.zlogErrorDefect( WARNING, s"Update/assign for feed ${fi.feedId}" )

  def updateAssignItems( ds : DataSource ) : Task[Unit] =
    for
      feedInfos <- withConnectionTransactional( ds )( conn => LatestSchema.Table.Feed.selectAll( conn ) )
      _         <- ZIO.collectAllParDiscard( feedInfos.map( fi => resilientDelayedForFeedInfo(ds,fi) ) )
    yield ()

  def completeAssignables( ds : DataSource, apiLinkGenerator : ApiLinkGenerator ) : Task[Unit] =
    withConnectionTransactional( ds ): conn =>
      LatestSchema.Table.Assignable.selectAllKeys( conn ).foreach: ak =>
        val AssignableKey( subscribableName, withinTypeId ) = ak
        val count = LatestSchema.Table.Assignment.selectCountWithinAssignable( conn, subscribableName, withinTypeId )
        val ( feedId, subscriptionManager ) = LatestSchema.Table.Subscribable.selectFeedIdAndManager( conn, subscribableName )
        val lastAssigned = LatestSchema.Table.Feed.selectLastAssigned( conn, feedId ).getOrElse:
          throw new AssertionError( s"DB constraints should have ensured a row for feed with ID '${feedId}' with a NOT NULL lastAssigned, but did not?" )
        if subscriptionManager.isComplete( conn, withinTypeId, count, lastAssigned ) then
          route( conn, ak, subscriptionManager, apiLinkGenerator )
          LatestSchema.Table.Subscribable.updateLastCompletedWti( conn, subscribableName, withinTypeId )
          cleanUpCompleted( conn, subscribableName, withinTypeId )
          INFO.log( s"Completed assignable '${withinTypeId}' with subscribable '${subscribableName}'." )
          INFO.log( s"Cleaned away data associated with completed assignable '${withinTypeId}' in subscribable '${subscribableName}'." )

  def cleanUpCompleted( conn : Connection, subscribableName : SubscribableName, withinTypeId : String ) : Unit =
    LatestSchema.Table.Assignment.cleanAwayAssignable( conn, subscribableName, withinTypeId )
    LatestSchema.Table.Assignable.delete( conn, subscribableName, withinTypeId )
    LatestSchema.Join.ItemAssignableAssignment.clearOldCache( conn ) // sets item status to ItemAssignability.Cleared for all fully-distributed items
    DEBUG.log( s"Assignable (item collection) defined by subscribable name '${subscribableName}', within-type-id '${withinTypeId}' has been deleted." )
    DEBUG.log( "Cached values of items fully distributed have been cleared." )

  def addFeed( ds : DataSource, nf : NascentFeed ) : Task[Set[FeedInfo]] =
    withConnectionTransactional( ds ): conn =>
      val newFeedId = LatestSchema.Table.Feed.Sequence.FeedSeq.selectNext(conn)
      LatestSchema.Table.Feed.insert(conn, newFeedId, nf)
      try
        val fd = FeedDigest( nf.feedUrl )
        fd.guidToItemContent.foreach: (guid, itemContent) =>
          LatestSchema.Table.Item.insertNew( conn, newFeedId, guid, None, ItemAssignability.Excluded ) // don't cache items we're excluding anyway
          DEBUG.log( s"Pre-existing item with GUID '${guid}' from new feed with ID ${newFeedId} has been excluded from distribution." )
      catch
        case NonFatal(t) =>
          WARNING.log(s"Failed to exclude existing content from assignment when adding feed '${nf.feedUrl}'. Existing content may be distributed.", t)
      LatestSchema.Table.Feed.selectAll(conn)

  def listFeeds( ds : DataSource ) : Task[Set[FeedInfo]] =
    withConnectionTransactional( ds ): conn =>
      LatestSchema.Table.Feed.selectAll(conn)

  def fetchExcluded( ds : DataSource ) : Task[Set[ExcludedItem]] =
    withConnectionTransactional( ds ): conn =>
      LatestSchema.Table.Item.selectExcluded(conn)

  def addSubscribable( ds : DataSource, subscribableName : SubscribableName, feedId : FeedId, subscriptionManager : SubscriptionManager ) : Task[Unit] =
    withConnectionTransactional( ds ): conn =>
      LatestSchema.Table.Subscribable.insert( conn, subscribableName, feedId, subscriptionManager, None )
      INFO.log( s"New subscribable '${subscribableName}' defined on feed with ID ${feedId}." )

  def addSubscription( ds : DataSource, fromExternalApi : Boolean, subscribableName : SubscribableName, destinationJson : Destination.Json, confirmed : Boolean, now : Instant ) : Task[(SubscriptionManager,SubscriptionId)] =
    withConnectionTransactional( ds )( conn => addSubscription( conn, fromExternalApi, subscribableName, destinationJson, confirmed, now ) )

  def addSubscription( ds : DataSource, fromExternalApi : Boolean, subscribableName : SubscribableName, destination : Destination, confirmed : Boolean, now : Instant ) : Task[(SubscriptionManager,SubscriptionId)] =
    withConnectionTransactional( ds )( conn => addSubscription( conn, fromExternalApi, subscribableName, destination, confirmed, now ) )

  def addSubscription( conn : Connection, fromExternalApi : Boolean, subscribableName : SubscribableName, destinationJson : Destination.Json, confirmed : Boolean, now : Instant ) : (SubscriptionManager,SubscriptionId) =
    val subscriptionManager = LatestSchema.Table.Subscribable.selectManager( conn, subscribableName )
    val destination = subscriptionManager.materializeDestination( destinationJson )
    val newId = addSubscription( conn, fromExternalApi, subscribableName, subscriptionManager, destination, confirmed, now )
    (subscriptionManager, newId)

  def addSubscription( conn : Connection, fromExternalApi : Boolean, subscribableName : SubscribableName, destination : Destination, confirmed : Boolean, now : Instant ) : (SubscriptionManager,SubscriptionId) =
    val subscriptionManager = LatestSchema.Table.Subscribable.selectManager( conn, subscribableName )
    val newId = addSubscription( conn, fromExternalApi, subscribableName, subscriptionManager, destination, confirmed, now )
    (subscriptionManager, newId)

  def addSubscription( conn : Connection, fromExternalApi : Boolean, subscribableName : SubscribableName, subscriptionManager : SubscriptionManager, destination : Destination, confirmed : Boolean, now : Instant ) : SubscriptionId =
    subscriptionManager.validateSubscriptionOrThrow( conn, fromExternalApi, destination, subscribableName )
    val newId = LatestSchema.Table.Subscription.Sequence.SubscriptionSeq.selectNext( conn )
    LatestSchema.Table.Subscription.insert( conn, newId, destination, subscribableName, confirmed, now )
    INFO.log:
      val status = if confirmed then "confirmed" else "unconfirmed"
      s"New ${status} subscription to '${subscribableName}' created for destination '${destination.shortDesc}'."
    newId

  def listSubscribables( ds : DataSource ) : Task[Set[(SubscribableName,FeedId,SubscriptionManager,Option[String])]] =
    withConnectionTransactional( ds ): conn =>
      LatestSchema.Table.Subscribable.select( conn )

  object Config:
    def fetchValue( conn : Connection, key : ConfigKey ) : Option[String] = LatestSchema.Table.Config.select( conn, key )
    def zfetchValue( conn : Connection, key : ConfigKey ) : Task[Option[String]] = ZIO.attemptBlocking( LatestSchema.Table.Config.select( conn, key ) )
    def confirmHours( conn : Connection ) : Int = fetchValue( conn, ConfigKey.ConfirmHours ).map( _.toInt ).getOrElse( Default.Config.ConfirmHours )
    def dumpDbDir( conn : Connection ) : os.Path = fetchValue( conn, ConfigKey.DumpDbDir ).map( os.Path.apply ).getOrElse( throw new ConfigurationMissing( ConfigKey.DumpDbDir ) )
    def timeZone( conn : Connection ) : ZoneId = fetchValue( conn, ConfigKey.TimeZone ).map( str => ZoneId.of(str) ).getOrElse( ZoneId.systemDefault() )
    def mailBatchDelaySeconds( conn : Connection ) : Int = fetchValue( conn, ConfigKey.MailBatchDelaySeconds ).map( _.toInt ).getOrElse( Default.Config.MailBatchDelaySeconds )
    def mailMaxRetries( conn : Connection ) : Int = fetchValue( conn, ConfigKey.MailMaxRetries ).map( _.toInt ).getOrElse( Default.Config.MailMaxRetries )
    def mastodonMaxRetries( conn : Connection ) : Int = fetchValue( conn, ConfigKey.MastodonMaxRetries ).map( _.toInt ).getOrElse( Default.Config.MastodonMaxRetries )
    def mailBatchSize( conn : Connection ) : Int = fetchValue( conn, ConfigKey.MailBatchSize ).map( _.toInt ).getOrElse( Default.Config.MailBatchSize )
    def webDaemonPort( conn : Connection ) : Int = fetchValue( conn, ConfigKey.WebDaemonPort ).map( _.toInt ).getOrElse( Default.Config.WebDaemonPort )
    def webDaemonInterface( conn : Connection ) : String = fetchValue( conn, ConfigKey.WebDaemonInterface ).getOrElse( Default.Config.WebDaemonInterface )
    def webApiProtocol( conn : Connection ) : String = fetchValue( conn, ConfigKey.WebApiProtocol ).getOrElse( Default.Config.WebApiProtocol )
    def webApiHostName( conn : Connection ) : String = fetchValue( conn, ConfigKey.WebApiHostName ).getOrElse( Default.Config.WebApiHostName )
    def webApiBasePath( conn : Connection ) : String = fetchValue( conn, ConfigKey.WebApiBasePath ).getOrElse( Default.Config.WebApiBasePath )
    def webApiPort( conn : Connection ) : Option[Int] = fetchValue( conn, ConfigKey.WebApiPort ).map( _.toInt ) orElse Default.Config.WebApiPort

  def webApiUrlBasePath( conn : Connection ) : (String, List[String]) =
    lazy val wdi = Config.webDaemonInterface(conn)
    lazy val wdp = Config.webDaemonPort(conn)

    val wapr = Config.webApiProtocol(conn)
    val wahn = Config.webApiHostName(conn)
    val wabp = Config.webApiBasePath(conn)

    val wapo = Config.webApiPort(conn).orElse:
      // special case... if we're under default localhost config, include the bound port directly for testing
      // otherwise assume we are behind a proxy server, and no port should be inserted
      if wahn == "localhost" && wdi == "127.0.0.1" then Some(wdp) else None

    val portPart = wapo.fold("")(p => s":$p")
    val url = s"${wapr}://${wahn}${portPart}/"
    val bp = wabp.split('/').filter( _.nonEmpty ).toList
    ( url, bp )

  def webApiUrlBasePath( ds : DataSource ) : Task[(String, List[String])] = withConnectionTransactional( ds )( webApiUrlBasePath )

  def confirmHours( ds : DataSource ) : Task[Int] = withConnectionTransactional( ds )( Config.confirmHours )

  def pullMailGroup( conn : Connection ) : Set[MailSpec.WithTemplate] =
    val batchSize = Config.mailBatchSize( conn )
    val withHashes : Set[MailSpec.WithHash] = LatestSchema.Table.Mailable.selectForDelivery(conn, batchSize)
    val contentMap = mutable.Map.empty[Hash.SHA3_256,String]
    def templateFromHash( hash : Hash.SHA3_256 ) : String =
      LatestSchema.Table.MailableTemplate.selectByHash( conn, hash ).getOrElse:
        throw new AssertionError(s"Database consistency issue, we should only be trying to load contents from extant hashes. (${hash.hex})")
    withHashes.foreach: mswh =>
      contentMap.getOrElseUpdate( mswh.templateHash, templateFromHash(mswh.templateHash) )
    withHashes.map( mswh => MailSpec.WithTemplate( mswh.seqnum, mswh.templateHash, templateFromHash( mswh.templateHash ), mswh.from, mswh.replyTo, mswh.to, mswh.subject, mswh.templateParams, mswh.retried ) )

  def attemptMail( conn : Connection, maxRetries : Int, mswt : MailSpec.WithTemplate, smtpContext : Smtp.Context ) : Unit =
    LatestSchema.Table.Mailable.deleteSingle( conn, mswt.seqnum )
    var attemptDeleteContents = true
    try
      val contents = mswt.templateParams.fill( mswt.template )
      given Smtp.Context = smtpContext
      Smtp.sendSimpleHtmlOnly( contents, subject = mswt.subject, from = mswt.from.toString(), to = mswt.to.toString(), replyTo = mswt.replyTo.map(_.toString()).toSeq )
      INFO.log(s"Mail sent from '${mswt.from}' to '${mswt.to}' with subject '${mswt.subject}'")
    catch
      case NonFatal(t) =>
        val lastRetryMessage = if mswt.retried == maxRetries then "(last retry, will drop)" else s"(maxRetries: ${maxRetries})"
        WARNING.log( s"Failed email attempt: subject = ${mswt.subject}, from = ${mswt.from}, to = ${mswt.to}, replyTo = ${mswt.replyTo} ), retried = ${mswt.retried} ${lastRetryMessage}", t )
        LatestSchema.Table.Mailable.insert( conn, mswt.templateHash, mswt.from, mswt.replyTo, mswt.to, mswt.subject, mswt.templateParams, mswt.retried + 1)
        attemptDeleteContents = false
    if attemptDeleteContents then
      LatestSchema.Table.MailableTemplate.deleteIfUnreferenced( conn, mswt.templateHash )

  def mailNextGroup( conn : Connection, smtpContext : Smtp.Context ) =
    val retries = Config.mailMaxRetries( conn )
    val mswts   = pullMailGroup( conn )
    mswts.foreach( mswt => attemptMail( conn, retries, mswt, smtpContext ) )

  def mailNextGroup( ds : DataSource, smtpContext : Smtp.Context ) : Task[Unit] =
    withConnectionTransactional( ds ): conn =>
      mailNextGroup( conn, smtpContext )

  def hasMetadataTable( conn : Connection ) : Boolean =
    Using.resource( conn.getMetaData().getTables(null,null,PgSchema.Unversioned.Table.Metadata.Name,null) )( _.next() )

  def ensureMetadataTable( conn : Connection ) : Task[Unit] =
    ZIO.attemptBlocking:
      if !hasMetadataTable(conn) then throw new DbNotInitialized("Please initialize the database. (No metadata table found.)")

  def ensureDb( ds : DataSource ) : Task[Unit] =
    withConnectionZIO( ds ): conn =>
      for
        _ <- ensureMetadataTable(conn)
        mbSchemaVersion <- zfetchMetadataValue(conn, MetadataKey.SchemaVersion).map( option => option.map( _.toInt ) )
        mbAppVersion <- zfetchMetadataValue(conn, MetadataKey.CreatorAppVersion)
      yield
        mbSchemaVersion match
          case Some( schemaVersion ) =>
            if schemaVersion > LatestSchema.Version then
              throw new MoreRecentFeedletterVersionRequired(
                s"The database schema version is ${schemaVersion}. " +
                mbAppVersion.fold("")( appVersion => s"It was created by app version '${appVersion}'. " ) +
                s"The latest version known by this version of the app is ${LatestSchema.Version}. " +
                s"You are running app version '${BuildInfo.version}'."
              )
            else if schemaVersion < LatestSchema.Version then
              throw new SchemaMigrationRequired(
                s"The database schema version is ${schemaVersion}. " +
                mbAppVersion.fold("")( appVersion => s"It was created by app version '${appVersion}'. " ) +
                s"The current schema this version of the app (${BuildInfo.version}) is ${LatestSchema.Version}. " +
                "Please migrate."
              )
            // else schemaVersion == LatestSchema.version and we're good
          case None =>
            throw new DbNotInitialized("Please initialize the database.")
  end ensureDb

  def feedUrl( conn : Connection, feedId : FeedId ) : Option[FeedUrl] =
    LatestSchema.Table.Feed.selectUrl(conn, feedId)

  def assertFeedUrl( conn : Connection, feedId : FeedId ) : FeedUrl =
    LatestSchema.Table.Feed.selectUrl(conn, feedId).getOrElse:
      throw new FeedletterException( s"Expected a URL assigned for Feed ID '$feedId', none found.")

  def feedIdUrlForSubscribableName( conn : Connection, subscribableName : SubscribableName ) : ( FeedId, FeedUrl ) =
    LatestSchema.Join.ItemSubscribable.selectFeedIdUrlForSubscribableName( conn, subscribableName )

  def subscriptionManagerForSubscribableName( conn : Connection, subscribableName : SubscribableName ) : SubscriptionManager =
    LatestSchema.Table.Subscribable.selectManager( conn, subscribableName )

  def subscriptionManagerForSubscribableName( ds : DataSource, subscribableName : SubscribableName ) : Task[SubscriptionManager] =
    withConnectionTransactional( ds )( subscriptionManagerForSubscribableName( _, subscribableName ) )

  def feedUrlSubscriptionManagerForSubscribableName( conn : Connection, subscribableName : SubscribableName ) : ( FeedUrl, SubscriptionManager ) =
    LatestSchema.Join.ItemSubscribable.selectFeedUrlSubscriptionManagerForSubscribableName( conn, subscribableName )

  def feedUrlSubscriptionManagerForSubscribableName( ds : DataSource, subscribableName : SubscribableName ) : Task[( FeedUrl, SubscriptionManager )] =
    withConnectionTransactional( ds )( feedUrlSubscriptionManagerForSubscribableName( _, subscribableName ) )

  def updateSubscriptionManagerJson( conn : Connection, subscribableName : SubscribableName, subscriptionManager : SubscriptionManager ) =
    LatestSchema.Table.Subscribable.updateSubscriptionManagerJson( conn, subscribableName, subscriptionManager )

  def updateSubscriptionManagerJson( ds : DataSource, subscribableName : SubscribableName, subscriptionManager : SubscriptionManager ) : Task[Unit] =
    withConnectionTransactional( ds )( conn => updateSubscriptionManagerJson(conn, subscribableName, subscriptionManager) )

  def updateConfirmed( conn : Connection, subscriptionId : SubscriptionId, confirmed : Boolean ) : Unit =
    LatestSchema.Table.Subscription.updateConfirmed( conn, subscriptionId, confirmed )
    INFO.log:
      val status = if confirmed then "confirmed" else "unconfirmed"
      s"Subscription with ID ${subscriptionId} has been marked ${status}."

  def updateConfirmed( ds : DataSource, subscriptionId : SubscriptionId, confirmed : Boolean ) : Task[Unit] =
    withConnectionTransactional( ds )( conn => updateConfirmed(conn, subscriptionId, confirmed) )

  def subscriptionInfoForSubscriptionId( conn : Connection, id : SubscriptionId ) : Option[SubscriptionInfo] =
    LatestSchema.Join.SubscribableSubscription.selectSubscriptionInfoForSubscriptionId( conn, id )

  def unsubscribe( conn : Connection, id : SubscriptionId ) : Option[SubscriptionInfo] =
    val out = subscriptionInfoForSubscriptionId( conn, id )
    out.foreach: _ =>
      LatestSchema.Table.Subscription.delete( conn, id )
      INFO.log( s"Subscription with ID ${id} removed." )
    out

  def webDaemonBinding( ds : DataSource ) : Task[(String,Int)] =
    withConnectionTransactional( ds ): conn =>
      ( Config.webDaemonInterface( conn ), Config.webDaemonPort( conn ) )

  def uninterpretedManagerJsonForSubscribableName( ds : DataSource, subscribableName : SubscribableName ) : Task[String] =
    withConnectionTransactional( ds ): conn =>
      LatestSchema.Table.Subscribable.selectUninterpretedManagerJson( conn, subscribableName )

  def checkMustReloadDaemon( conn : Connection ) : Boolean = LatestSchema.Table.Flags.isSet( conn, Flag.MustReloadDaemon )
  def clearMustReloadDaemon( conn : Connection ) : Unit    = LatestSchema.Table.Flags.unset( conn, Flag.MustReloadDaemon )
  def setMustReloadDaemon  ( conn : Connection ) : Unit    = LatestSchema.Table.Flags.set  ( conn, Flag.MustReloadDaemon )

  def checkMustReloadDaemon( ds : DataSource ) : Task[Boolean] = withConnectionTransactional(ds)( checkMustReloadDaemon )
  def clearMustReloadDaemon( ds : DataSource ) : Task[Unit]    = withConnectionTransactional(ds)( clearMustReloadDaemon )

  def expireUnconfirmed( conn : Connection ) : Unit =
    val confirmHours = math.max(Config.confirmHours( conn ), 0)
    DEBUG.log( s"Expiring unconfirmed subscriptions at least ${confirmHours} hours old." )
    val expiredThreshold = Instant.now().minusSeconds( confirmHours * 60 )
    val subscriptionsExpired = LatestSchema.Table.Subscription.expireUnconfirmedAddedBefore( conn, expiredThreshold )
    if subscriptionsExpired > 0 then
      INFO.log( s"Expired ${subscriptionsExpired} unconfirmed subscriptions at least ${confirmHours} hours old." )

  def expireUnconfirmed( ds : DataSource ) : Task[Unit] = withConnectionTransactional( ds )( expireUnconfirmed )

  def queueForMastoPost( conn : Connection, fullContent : String, mastoInstanceUrl : MastoInstanceUrl, mastoName : MastoName, media : Seq[ItemContent.Media] ) =
    val id = LatestSchema.Table.MastoPostable.Sequence.MastoPostableSeq.selectNext( conn )
    LatestSchema.Table.MastoPostable.insert( conn, id, fullContent, mastoInstanceUrl, mastoName, 0 )
    (0 until media.size).foreach: i =>
      LatestSchema.Table.MastoPostableMedia.insert( conn, id, i, media(i) )

  def notifyAllMastoPosts( conn : Connection, appSetup : AppSetup ) =
    val retries = Config.mastodonMaxRetries( conn )
    LatestSchema.Table.MastoPostable.foreach( conn ): mastoPostable =>
      attemptMastoPost( conn, appSetup, retries, mastoPostable )

  def attemptMastoPost( conn : Connection, appSetup : AppSetup, maxRetries : Int, mastoPostable : MastoPostable ) : Boolean =
    LatestSchema.Table.MastoPostable.delete(conn, mastoPostable.id)
    try
      mastoPost( appSetup, mastoPostable )
      INFO.log( s"Notification posted to Mastodon destination ${mastoPostable.instanceUrl}." )
      true
    catch
      case NonFatal(t) =>
        val lastRetryMessage = if mastoPostable.retried == maxRetries then "(last retry, will drop)" else s"(maxRetries: ${maxRetries})"
        WARNING.log( s"Failed attempt to post to Mastodon destination '${mastoPostable.instanceUrl}', retried = ${mastoPostable.retried} ${lastRetryMessage}", t )
        val newId = LatestSchema.Table.MastoPostable.Sequence.MastoPostableSeq.selectNext( conn )
        LatestSchema.Table.MastoPostable.insert( conn, newId, mastoPostable.finalContent, mastoPostable.instanceUrl, mastoPostable.name, mastoPostable.retried + 1 )
        false

