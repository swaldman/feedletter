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

import LoggingApi.*

import audiofluidity.rss.util.formatPubDate

import com.mchange.sc.sqlutil.*
import com.mchange.sc.sqlutil.migrate.*
import com.mchange.sc.zsqlutil.*
import com.mchange.sc.zsqlutil.zmigrate.*

import com.mchange.mailutil.*
import com.mchange.cryptoutil.{*, given}

import com.mchange.feedletter.BuildInfo
import com.mchange.feedletter.api.ApiLinkGenerator

object PgDatabase extends ZMigratory.Postgres[PgSchema.V2.type], SelfLogging:
  val MinMastoPostSpacing = 2.minutes // should be configurable, but for now...
  val MinBskyPostSpacing  = 2.minutes // should be configurable, but for now...

  override val DumpFileAppDbTag  = "feedletter-pg"
  override val LatestSchema      = PgSchema.V2
  override val MetadataTableName = PgSchema.Unversioned.Table.Metadata.Name

  lazy val targetDbVersion : Int = LatestSchema.Version

  override def getRunningAppVersionIfAvailable() : Option[String] = Some( BuildInfo.version )

  override def fetchMetadataValue( conn : Connection, key : MetadataKey ) : Option[String] =
    PgSchema.Unversioned.Table.Metadata.select( conn, key )

  override def hasMetadataTable( conn : Connection ) : Boolean =
    Using.resource( conn.getMetaData().getTables(null,null,PgSchema.Unversioned.Table.Metadata.Name,null) )( _.next() )

  override def fetchDumpDir(conn : Connection) : Task[Option[os.Path]] = Config.zfetchValue(conn, ConfigKey.DumpDbDir).map( opt => opt.map(os.Path(_)) )

  override def runDump( ds : DataSource, mbDbName : Option[String], dumpFile : os.Path ) : Task[Unit] = simpleLocalRunDump( ds, mbDbName, dumpFile )

  override def upMigrate(conn : Connection, from : Option[Int]) : Task[Unit] =
    def upMigrateFrom_New() : Task[Unit] =
      ZIO.attemptBlocking:
        TRACE.log( "upMigrateFrom_New()" )
        PgSchema.Unversioned.Table.Metadata.create( conn )
        insertMetadataKeys(
          conn,
          (MetadataKey.SchemaVersion, "0"),
          (MetadataKey.CreatorAppVersion, BuildInfo.version)
        )

    def upMigrateFrom_0() : Task[Unit] =
      ZIO.attemptBlocking:
        TRACE.log( "upMigrateFrom_0()" )
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
          PgSchema.V1.Table.ImmediatelyMailable.create( stmt )
          PgSchema.V1.Table.ImmediatelyMailable.Sequence.ImmediatelyMailableSeq.create( stmt )
          PgSchema.V1.Table.MastoPostable.create( stmt )
          PgSchema.V1.Table.MastoPostable.Sequence.MastoPostableSeq.create( stmt )
          PgSchema.V1.Table.MastoPostableMedia.create( stmt )
        updateMetadataKeys(
          conn,
          (MetadataKey.SchemaVersion, "1"),
          (MetadataKey.CreatorAppVersion, BuildInfo.version)
        )

    def upMigrateFrom_1() : Task[Unit] =
      ZIO.attemptBlocking:
        TRACE.log( "upMigrateFrom_1()" )
        Using.resource( conn.createStatement() ): stmt =>
          PgSchema.V2.Table.BskyPostable.create( stmt )
          PgSchema.V2.Table.BskyPostable.Sequence.BskyPostableSeq.create( stmt )
          PgSchema.V2.Table.BskyPostableMedia.create( stmt )
        updateMetadataKeys(
          conn,
          (MetadataKey.SchemaVersion, "2"),
          (MetadataKey.CreatorAppVersion, BuildInfo.version)
        )

    TRACE.log( s"upMigrate( from=${from} )" )
    from match
      case None      => upMigrateFrom_New()
      case Some( 0 ) => upMigrateFrom_0()
      case Some( 1 ) => upMigrateFrom_1()
      case Some( `targetDbVersion` ) =>
        ZIO.fail( new CannotUpMigrate( s"Cannot upmigrate from current target DB version: V${targetDbVersion}" ) )
      case Some( other ) =>
        ZIO.fail( new CannotUpMigrate( s"Cannot upmigrate from unknown DB version: V${other}" ) )

  override def insertMetadataKeys( conn : Connection, pairs : (MetadataKey,String)* ) : Unit =
    pairs.foreach( ( mdkey, value ) => PgSchema.Unversioned.Table.Metadata.insert(conn, mdkey, value) )

  override def updateMetadataKeys( conn : Connection, pairs : (MetadataKey,String)* ) : Unit =
    pairs.foreach( ( mdkey, value ) => PgSchema.Unversioned.Table.Metadata.update(conn, mdkey, value) )

  private def insertConfigKeys( conn : Connection, pairs : (ConfigKey,String)* ) : Unit =
    pairs.foreach( ( cfgkey, value ) => LatestSchema.Table.Config.insert(conn, cfgkey, value) )
    setFlag(conn, Flag.MustReloadDaemon)

  private def updateConfigKeys( conn : Connection, pairs : (ConfigKey,String)* ) : Unit =
    pairs.foreach( ( cfgkey, value ) => LatestSchema.Table.Config.update(conn, cfgkey, value) )
    setFlag(conn, Flag.MustReloadDaemon)

  private def upsertConfigKeys( conn : Connection, pairs : (ConfigKey,String)* ) : Unit =
    pairs.foreach( ( cfgkey, value ) => LatestSchema.Table.Config.upsert(conn, cfgkey, value) )
    setFlag(conn, Flag.MustReloadDaemon)

  private def sort( tups : Set[Tuple2[ConfigKey,String]] ) : immutable.SortedSet[Tuple2[ConfigKey,String]] =
    immutable.SortedSet.from( tups )( using Ordering.by( tup => (tup(0).toString().toUpperCase, tup(1) ) ) )

  private def upsertConfigKeyMapAndReport( conn : Connection, map : Map[ConfigKey,String] ) : immutable.SortedSet[Tuple2[ConfigKey,String]] =
    upsertConfigKeys( conn, map.toList* )
    reportAllConfigKeysStringified( conn )

  def upsertConfigKeyMapAndReport( ds : DataSource, map : Map[ConfigKey,String] ) : Task[immutable.SortedSet[Tuple2[ConfigKey,String]]] =
    withConnectionTransactional( ds )( conn => upsertConfigKeyMapAndReport( conn, map ) )

  def reportNondefaultConfigKeys( ds : DataSource ): Task[immutable.SortedSet[Tuple2[ConfigKey,String]]] =
    withConnectionTransactional( ds )( conn => sort( LatestSchema.Table.Config.selectTuples( conn ) ) )

  def reportAllConfigKeysStringified( conn : Connection ): immutable.SortedSet[Tuple2[ConfigKey,String]] =
    val stringifyThrowable : PartialFunction[Throwable,String] = {
      case NonFatal(t) => "throws " + t.getClass().getName()
    }
    val tups = ConfigKey.values.map: ck =>
      val v = try PgDatabase.Config.fetchByKey( conn, ck ).toString() catch stringifyThrowable
      ( ck, v )
    sort( tups.toSet )

  def reportAllConfigKeysStringified( ds : DataSource ): Task[immutable.SortedSet[Tuple2[ConfigKey,String]]] =
    withConnectionTransactional( ds )( reportAllConfigKeysStringified )

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
    TRACE.log( s"assignForSubscribable( $conn, $subscribableName, $feedId, $guid, $content, $status )" )
    val subscriptionManager = LatestSchema.Table.Subscribable.selectManager( conn, subscribableName )
    subscriptionManager.withinTypeId( conn, subscribableName, feedId, guid, content, status ) match
      case Some(wti) =>
        ensureOpenAssignable( conn, feedId, subscribableName, wti, Some(guid) )
        LatestSchema.Table.Assignment.insert( conn, subscribableName, wti, guid )
        DEBUG.log( s"Item with GUID '${guid}' from feed with ID ${feedId} has been assigned in subscribable '${subscribableName}' with assignable identifier '${wti}'." )
      case None =>
        INFO.log( s"Subscribable '${subscribableName}' determined that item with guid '${guid}' should not be announced to its subscribers." )

  private def assign( conn : Connection, feedId : FeedId, guid : Guid, content : ItemContent, status : ItemStatus ) : Unit =
    TRACE.log( s"assign( $conn, $feedId, $guid, $content, $status )" )
    val subscribableNames = LatestSchema.Table.Subscribable.selectSubscribableNamesByFeedId( conn, feedId )
    subscribableNames.foreach( subscribableName => assignForSubscribable( conn, subscribableName, feedId, guid, content, status ) )
    LatestSchema.Table.Item.updateLastCheckedAssignability( conn, feedId, guid, status.lastChecked, ItemAssignability.Assigned )
    DEBUG.log( s"Item with GUID '${guid}' from feed with ID ${feedId} has been assigned (or refused assignment) in all subscribables to that feed." )

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
  //
  // reverse chronological by first-seen time
  private def materializeAssignable( conn : Connection, feedId : FeedId, assignableKey : AssignableKey ) : Seq[ItemContent] =
    LatestSchema.Join.ItemAssignment.selectItemContentsForAssignable( conn, feedId, assignableKey.subscribableName, assignableKey.withinTypeId )

  private def route( conn : Connection, feedId : FeedId, assignableKey : AssignableKey, sman : SubscriptionManager, apiLinkGenerator : ApiLinkGenerator ) : Unit =
    val AssignableKey( subscribableName, withinTypeId ) = assignableKey
    val contents = materializeAssignable(conn, feedId, assignableKey)
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

  def mailImmediately(
    conn           : Connection,
    as             : AppSetup,
    contents       : String,
    from           : AddressHeader[From],
    replyTo        : Option[AddressHeader[ReplyTo]],
    to             : AddressHeader[To],
    templateParams : TemplateParams,
    subject        : String
  ) : Unit =
    LatestSchema.Table.ImmediatelyMailable.insert(conn, contents, from, replyTo, to, templateParams, subject )
    DEBUG.log( s"Template queued for immediate mailing, from '${from}', to '${to}', with subject '${subject}'." )
    setFlag(conn, Flag.ImmediateMailQueued)

  def queueForMailing( conn : Connection, contents : String, from : AddressHeader[From], replyTo : Option[AddressHeader[ReplyTo]], tosWithParams : Set[(AddressHeader[To],TemplateParams)], subject : String ) : Unit =
    if tosWithParams.nonEmpty then
      val hash = Hash.SHA3_256.hash( contents.getBytes( scala.io.Codec.UTF8.charSet ) )
      LatestSchema.Table.MailableTemplate.ensure( conn, hash, contents )
      LatestSchema.Table.Mailable.insertBatch( conn, hash, from, replyTo, tosWithParams, subject, 0 )
      FINE.log( s"Template queued for batched mailing, from '${from}', to ${tosWithParams.size} recipients, with subject '${subject}'." )
    else
      FINE.log( s"Template NOT queued for batched mailing, from '${from}' with subject '${subject}', because no recipients were specified." )


  private def resilientDelayedForFeedInfo( ds : DataSource, fi : FeedInfo ) : Task[Unit] =
    val simple = withConnectionTransactional( ds )( conn => updateAssignItems( conn, fi ) ).zlogError( DEBUG, what = s"Attempt to update/assign for ${fi}" )
    val retrying = simple.retry( Schedule.exponential( 10.seconds, 1.5f ) && Schedule.upTo( 3.minutes ) ) // XXX: hard-coded
    val delaying =
      for
        _ <- ZIO.sleep( math.round(math.random * 20).seconds ) // XXX: hard-coded
        _ <- retrying
      yield()
    delaying.zlogErrorDefect( WARNING, s"Update/assign for feed ${fi.feedId} (${fi.feedUrl})" )

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
        val feedLastAssigned = LatestSchema.Table.Feed.selectLastAssigned( conn, feedId ).getOrElse:
          throw new AssertionError( s"DB constraints should have ensured a row for feed with ID '${feedId}' with a NOT NULL lastAssigned, but did not?" )
        if subscriptionManager.isComplete( conn, feedId, subscribableName, withinTypeId, count, feedLastAssigned ) then
          route( conn, feedId, ak, subscriptionManager, apiLinkGenerator )
          LatestSchema.Table.Subscribable.updateLastCompletedWti( conn, subscribableName, withinTypeId )
          cleanUpCompleted( conn, feedId, subscribableName, withinTypeId )
          INFO.log( s"Completed assignable '${withinTypeId}' with subscribable '${subscribableName}'." )
          INFO.log( s"Cleaned away data associated with completed assignable '${withinTypeId}' in subscribable '${subscribableName}'." )

  private def cleanUpCompleted( conn : Connection, feedId : FeedId, subscribableName : SubscribableName, withinTypeId : String ) : Unit =
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

  def addSubscribable( ds : DataSource, subscribableName : SubscribableName, feedId : FeedId, subscriptionManager : SubscriptionManager ) : Task[(SubscribableName,FeedId,SubscriptionManager,Option[String])] =
    withConnectionTransactional( ds ): conn =>
      LatestSchema.Table.Subscribable.insert( conn, subscribableName, feedId, subscriptionManager, None )
      INFO.log( s"New subscribable '${subscribableName}' defined on feed with ID ${feedId}." )
      ( subscribableName, feedId, subscriptionManager, None )

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

  def knownSubscribableNames( ds : DataSource) : Task[Set[SubscribableName]] = listSubscribables(ds).map( tupSet => tupSet.map(_(0)) )

  object Config:
    def fetchValue( conn : Connection, key : ConfigKey ) : Option[String] = LatestSchema.Table.Config.select( conn, key )
    def zfetchValue( conn : Connection, key : ConfigKey ) : Task[Option[String]] = ZIO.attemptBlocking( LatestSchema.Table.Config.select( conn, key ) )
    def blueskyMaxRetries( conn : Connection ) : Int = fetchValue( conn, ConfigKey.BlueskyMaxRetries ).map( _.toInt ).getOrElse( Default.Config.BlueskyMaxRetries )
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

    // may throw, if there's nothing set and no default! catch and handle accordingly!
    def fetchByKey( conn : Connection, key : ConfigKey ) : Any =
      key match
        case ConfigKey.BlueskyMaxRetries     => blueskyMaxRetries(conn)
        case ConfigKey.ConfirmHours          => confirmHours(conn)
        case ConfigKey.DumpDbDir             => dumpDbDir(conn)
        case ConfigKey.MailBatchSize         => mailBatchSize(conn)
        case ConfigKey.MailBatchDelaySeconds => mailBatchDelaySeconds(conn)
        case ConfigKey.MailMaxRetries        => mailMaxRetries(conn)
        case ConfigKey.MastodonMaxRetries    => mastodonMaxRetries(conn)
        case ConfigKey.TimeZone              => timeZone(conn)
        case ConfigKey.WebDaemonPort         => webDaemonPort(conn)
        case ConfigKey.WebDaemonInterface    => webDaemonInterface(conn)
        case ConfigKey.WebApiProtocol        => webApiProtocol(conn)
        case ConfigKey.WebApiHostName        => webApiHostName(conn)
        case ConfigKey.WebApiBasePath        => webApiBasePath(conn)
        case ConfigKey.WebApiPort            => webApiPort(conn)
        //no default! as we add keys, pay attention to the compiler and add an item here!

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

  def mailQueued( ds : DataSource ) : Task[Boolean] = withConnectionTransactional( ds )( LatestSchema.Table.Mailable.mailQueued )

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
      Smtp.sendSimpleHtmlOnly( contents, subject = mswt.subject, from = mswt.from.str, to = mswt.to.str, replyTo = mswt.replyTo.map(_.str).toSeq )
      INFO.log(s"Mail sent from '${mswt.from}' to '${mswt.to}' with subject '${mswt.subject}'")
    catch
      case NonFatal(t) =>
        val lastRetryMessage = if mswt.retried == maxRetries then "(last retry, will drop)" else s"(maxRetries: ${maxRetries})"
        WARNING.log( s"Failed email attempt: subject = ${mswt.subject}, from = ${mswt.from}, to = ${mswt.to}, replyTo = ${mswt.replyTo} ), retried = ${mswt.retried} ${lastRetryMessage}", t )
        if mswt.retried < maxRetries then
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

  def checkFlag( conn : Connection, flag : Flag ) : Boolean = LatestSchema.Table.Flags.isSet( conn, flag )
  def clearFlag( conn : Connection, flag : Flag ) : Unit    = LatestSchema.Table.Flags.unset( conn, flag )
  def setFlag  ( conn : Connection, flag : Flag ) : Unit    = LatestSchema.Table.Flags.set  ( conn, flag )

  def checkFlag( ds : DataSource, flag : Flag ) : Task[Boolean] = withConnectionTransactional(ds)( conn => checkFlag(conn,flag) )
  def clearFlag( ds : DataSource, flag : Flag ) : Task[Unit]    = withConnectionTransactional(ds)( conn => clearFlag(conn,flag) )

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
    FINE.log( s"Queued Mastodon post for distribution. Content: $fullContent" )

  def attemptMastoPost( conn : Connection, appSetup : AppSetup, maxRetries : Int, mastoPostable : MastoPostable ) : Boolean =
    FINE.log( s"Attempting Mastodon post, content: ${mastoPostable.finalContent}" )
    val media = mastoPostable.media
    if media.nonEmpty then
      LatestSchema.Table.MastoPostableMedia.deleteById(conn, mastoPostable.id)
    val deleted =
      LatestSchema.Table.MastoPostable.delete(conn, mastoPostable.id)
    try
      if deleted then
        val skipFailedMedia = mastoPostable.retried == maxRetries // iff this is our last attempt, skip media that fail to retrieve or post
        TRACE.log(s"Deleted masto-postable with id ${mastoPostable.id}. Attempting post.")
        Masto.post( appSetup, mastoPostable, skipFailedMedia )
        true
      else
        WARNING.log(s"While attempting to post Mastodon postable with id ${mastoPostable.id}, found it was no longer in the database, has apparently already been handled. Skipping post.")
        false
    catch
      case NonFatal(t) =>
        val lastRetryMessage = if mastoPostable.retried == maxRetries then "(last retry, will drop)" else s"(maxRetries: ${maxRetries})"
        WARNING.log( s"Failed attempt to post to Mastodon destination '${mastoPostable.instanceUrl}', retried = ${mastoPostable.retried} ${lastRetryMessage}", t )
        if mastoPostable.retried < maxRetries then
          val newId = LatestSchema.Table.MastoPostable.Sequence.MastoPostableSeq.selectNext( conn )
          LatestSchema.Table.MastoPostable.insert( conn, newId, mastoPostable.finalContent, mastoPostable.instanceUrl, mastoPostable.name, mastoPostable.retried + 1 )
          (0 until media.size).foreach: i =>
            LatestSchema.Table.MastoPostableMedia.insert( conn, newId, i, media(i) )
          TRACE.log(s"Reinserted masto-postable after failure, formerly with id ${mastoPostable.id}, updated to id ${newId}.")
        false
      case t : Throwable =>  
        WARNING.log(s"A fatal error occurred attempting to post Mastodon postable. Rethrowing! Content $mastoPostable.finalContent}", t)
        throw t

  // should NOT be executed within a transaction! handles transactions itself!
  def _parallelAcrossSpacedWithinAccountsNotifyAllPosts[POSTABLE](
    fetchAllPostables : Connection => Set[POSTABLE],
    accountDiscriminator : POSTABLE => Any,
    findSpacing : Connection => Duration,
    attemptPost : (Connection, AppSetup, Int, POSTABLE) => Boolean,
    findNumRetries : Connection => Int,
    serviceName : String,
    ds : DataSource,
    appSetup : AppSetup
  ) : Task[Unit] =
    val allWithins =
      withConnectionTransactional(ds): setupConn =>
        val retries = findNumRetries( setupConn )
        val allPostables = fetchAllPostables(setupConn)
        val byAccounts = allPostables.groupBy( accountDiscriminator )
        val spacing = findSpacing( setupConn )
        def timeSpaced( discriminator : Any, postables : Iterable[POSTABLE] ) : Task[Unit] =
          val sleepTask = ZIO.sleep( spacing )
          val postTasks : Vector[Task[Boolean]] =
            val iterable =
              postables.map: postable =>
                withConnectionTransactional(ds): postConn =>
                  val succeeded = attemptPost(postConn, appSetup, retries, postable)
                  if succeeded then
                      INFO.log(s"Posted ${serviceName} notification to ${discriminator}")
                      TRACE.log(s"Postable: ${postable}")
                  else
                    WARNING.log(s"Attempt to post ${serviceName} notification to ${discriminator} failed or was skipped. Postable: ${postable}")
                  succeeded
            iterable.toVector
          val intercolatedPostTasks : Vector[Task[Any]] =
            postTasks match
              case head +: tail => tail.foldLeft( Vector(head) : Vector[Task[Any]] )( (accum, next) => accum :+ sleepTask :+ next )
              case _            => Vector.empty
          ZIO.collectAllDiscard( intercolatedPostTasks )
        val out = byAccounts.map(timeSpaced)
        TRACE.log("Finished setting up sequences of tasks to post in spaced distinct, transactions")
        out
    for
      aw <- allWithins
      _  <- ZIO.collectAllParDiscard(aw)
    yield()  

  def notifyAllMastoPosts( ds : DataSource, appSetup : AppSetup ) =
    FINE.log( s"Notifying all queued Mastodon posts." )
    val fetchAllPostables = (conn : Connection) => LatestSchema.Table.MastoPostable.all(conn)
    val accountDiscriminator = (mastoPostable : MastoPostable) => Tuple2( mastoPostable.instanceUrl, mastoPostable.name )
    val findSpacing = (_ : Connection) => MinMastoPostSpacing
    val attemptPost = attemptMastoPost
    val findNumRetries = (conn : Connection) => Config.mastodonMaxRetries( conn )
    val serviceName = "Mastodon"
    _parallelAcrossSpacedWithinAccountsNotifyAllPosts[MastoPostable](
      fetchAllPostables,
      accountDiscriminator,
      findSpacing,
      attemptPost,
      findNumRetries,
      serviceName,
      ds,
      appSetup
    )

  def subscriptionsForSubscribableName( ds : DataSource, subscribableName : SubscribableName ) : Task[Set[( SubscriptionId, Destination, Boolean, Instant )]] =
    withConnectionTransactional( ds ): conn =>
      LatestSchema.Table.Subscription.selectForSubscribable( conn, subscribableName )

  def removeSubscribable( conn : Connection, subscribableName : SubscribableName, removeSubscriptionsIfNecessary : Boolean ) =
    if removeSubscriptionsIfNecessary then
      LatestSchema.Table.Subscription.deleteAllForSubscribable( conn, subscribableName )
    LatestSchema.Table.Assignment.cleanAwayAssignableAllAssignablesForSubscribable(conn, subscribableName)
    LatestSchema.Table.Assignable.deleteAllForSubscribable(conn, subscribableName)
    LatestSchema.Table.Subscribable.delete( conn, subscribableName )

  def removeSubscribable( ds : DataSource, subscribableName : SubscribableName, removeSubscriptionsIfNecessary : Boolean ) : Task[Unit] =
    withConnectionTransactional( ds ): conn =>
      removeSubscribable( conn, subscribableName, removeSubscriptionsIfNecessary )

  def subscribersExist( conn : Connection, subscribableName : SubscribableName ) : Boolean =
    LatestSchema.Table.Subscription.subscribersExist( conn, subscribableName )

  def subscribersExist( ds : DataSource, subscribableName : SubscribableName ) : Task[Boolean] =
    withConnectionTransactional( ds ): conn =>
      subscribersExist( conn, subscribableName )

  def cautiousRemoveSubscribable( conn : Connection, subscribableName : SubscribableName ) : Unit =
    val theyExist = PgDatabase.subscribersExist( conn, subscribableName )
    if theyExist then
      throw new WouldDropSubscriptions(s"Removing subscribable '${subscribableName.str}' would delete active subscriptions! Set remove-active-subscriptions flag if you wish to force deletion anyway.")
    else
      removeSubscribable(conn, subscribableName, false)

  def cautiousRemoveSubscribable( ds : DataSource, subscribableName : SubscribableName ) : Task[Unit] =
    withConnectionTransactional( ds ): conn =>
      cautiousRemoveSubscribable( conn, subscribableName )

  def removeFeedAndSubscribables( conn : Connection, feedId : FeedId, removeSubscriptionsIfNecessary : Boolean ) : Unit =
    val subscribables = LatestSchema.Table.Subscribable.selectByFeed( conn, feedId )
    if removeSubscriptionsIfNecessary then
      subscribables.foreach( subscribableName => removeSubscribable( conn, subscribableName, true ) )
    else
      subscribables.foreach( subscribableName => cautiousRemoveSubscribable(conn, subscribableName) )
    LatestSchema.Table.Item.deleteByFeed( conn, feedId )
    LatestSchema.Table.Feed.delete( conn, feedId )

  def removeFeedAndSubscribables( ds : DataSource, feedId : FeedId, removeSubscriptionsIfNecessary : Boolean ) : Task[Unit] =
    withConnectionTransactional( ds ): conn =>
      removeFeedAndSubscribables(conn,feedId,removeSubscriptionsIfNecessary)

  def updateFeedTimings( conn : Connection, feedId : FeedId, minDelayMinutes : Int, awaitStabilizationMinutes : Int, maxDelayMinutes : Int, assignEveryMinutes : Int ) : Unit =
    LatestSchema.Table.Feed.updateFeedTimings(conn, feedId, minDelayMinutes, awaitStabilizationMinutes, maxDelayMinutes, assignEveryMinutes)

  def updateFeedTimings( ds : DataSource, feedId : FeedId, minDelayMinutes : Int, awaitStabilizationMinutes : Int, maxDelayMinutes : Int, assignEveryMinutes : Int ) : Task[Unit] =
    withConnectionTransactional( ds ): conn =>
      updateFeedTimings( conn, feedId, minDelayMinutes, awaitStabilizationMinutes, maxDelayMinutes, assignEveryMinutes )

  def mergeFeedTimings( conn : Connection, fts : FeedTimings ) : Unit =
    val fi = LatestSchema.Table.Feed.selectById( conn, fts.feedId )
    val nextMinDelayMinutes = fts.minDelayMinutes.getOrElse( fi.minDelayMinutes )
    val nextAwaitStabilizationMinutes = fts.awaitStabilizationMinutes.getOrElse( fi.awaitStabilizationMinutes )
    val nextMaxDelayMinutes = fts.maxDelayMinutes.getOrElse( fi.maxDelayMinutes )
    val nextAssignEveryMinutes = fts.assignEveryMinutes.getOrElse( fi.assignEveryMinutes )
    updateFeedTimings( conn, fts.feedId, nextMinDelayMinutes, nextAwaitStabilizationMinutes, nextMaxDelayMinutes, nextAssignEveryMinutes )

  def mergeFeedTimings( ds : DataSource, fts : FeedTimings ) : Task[Unit] =
    withConnectionTransactional( ds ): conn =>
      mergeFeedTimings( conn, fts )

  def sendNextImmediatelyMailable( conn : Connection, as : AppSetup ) : Boolean =
    val mbim = LatestSchema.Table.ImmediatelyMailable.selectNext( conn )
    mbim match
      case Some( im ) =>
        LatestSchema.Table.ImmediatelyMailable.delete(conn, im) // if the transaction fails (as opposed to just the sending) we'll come back
        try
          val filledContents = im.templateParams.fill( im.contents )
          given Smtp.Context = as.smtpContext
          Smtp.sendSimpleHtmlOnly( im.contents, subject = im.subject, from = im.from.str, to = im.to.str, replyTo = im.replyTo.map(_.str).toSeq )
          INFO.log(s"Expedited mail sent from '${im.from}' to '${im.to}' with subject '${im.subject}'")
        catch
          case NonFatal(t) =>
            WARNING.log("Attempt to mail immediately from '${from}' to '${to}' with subject '${subject}' failed. Queuing to reattempt later.", t)
            queueForMailing(conn, im.contents, im.from, im.replyTo, im.to, im.templateParams, im.subject)
        true
      case None =>
        false

  def sendNextImmediatelyMailable( ds : DataSource, as : AppSetup ) : Task[Boolean] =
    withConnectionTransactional( ds ): conn =>
      sendNextImmediatelyMailable( conn, as )

  def destinationAlreadySubscribed( conn : Connection, destination : Destination, subscribableName : SubscribableName ) : Boolean =
    LatestSchema.Table.Subscription.destinationAlreadySubscribed( conn, destination, subscribableName )

  def queueForBskyPost( conn : Connection, fullContent : String, bskyEntrywayUrl : BskyEntrywayUrl, bskyIdentifier : BskyIdentifier, media : Seq[ItemContent.Media] ) =
    val id = LatestSchema.Table.BskyPostable.Sequence.BskyPostableSeq.selectNext( conn )
    LatestSchema.Table.BskyPostable.insert( conn, id, fullContent, bskyEntrywayUrl, bskyIdentifier, 0 )
    (0 until media.size).foreach: i =>
      LatestSchema.Table.BskyPostableMedia.insert( conn, id, i, media(i) )
    FINE.log( s"Queued BlueSky post for distribution. Content: $fullContent" )

  def attemptBskyPost( conn : Connection, appSetup : AppSetup, maxRetries : Int, bskyPostable : BskyPostable ) : Boolean =
    FINE.log( s"Attempting BlueSky post, content: ${bskyPostable.finalContent}" )
    val media = bskyPostable.media
    if media.nonEmpty then
      LatestSchema.Table.BskyPostableMedia.deleteById(conn, bskyPostable.id)
    val deleted =  
      LatestSchema.Table.BskyPostable.delete(conn, bskyPostable.id)
    try
      if deleted then
        TRACE.log(s"Deleted bsky-postable with id ${bskyPostable.id}. Attempting post.")
        Bsky.post( appSetup, bskyPostable )
        true
      else
        WARNING.log(s"While attempting to post BlueSky postable with id ${bskyPostable.id}, found it was no longer in the database, has apparently already been handled. Skipping post.")
        false
    catch
      case NonFatal(t) =>
        val lastRetryMessage = if bskyPostable.retried == maxRetries then "(last retry, will drop)" else s"(maxRetries: ${maxRetries})"
        WARNING.log( s"Failed attempt to post to Bluesky destination '${bskyPostable.entrywayUrl}', retried = ${bskyPostable.retried} ${lastRetryMessage}", t )
        if bskyPostable.retried < maxRetries then
          val newId = LatestSchema.Table.BskyPostable.Sequence.BskyPostableSeq.selectNext( conn )
          LatestSchema.Table.BskyPostable.insert( conn, newId, bskyPostable.finalContent, bskyPostable.entrywayUrl, bskyPostable.identifier, bskyPostable.retried + 1 )
          (0 until media.size).foreach: i =>
            LatestSchema.Table.BskyPostableMedia.insert( conn, newId, i, media(i) )
        false
      case t : Throwable =>  
        WARNING.log(s"A fatal error occurred attempting to post BlueSky postable. Rethrowing! Content ${bskyPostable.finalContent}", t)
        throw t

  def notifyAllBskyPosts( ds : DataSource, appSetup : AppSetup ) =
    FINE.log( "Notifying all queued BlueSky posts." )
    val fetchAllPostables = (conn : Connection) => LatestSchema.Table.BskyPostable.all(conn)
    val accountDiscriminator = ( bskyPostable : BskyPostable ) => Tuple2(bskyPostable.entrywayUrl, bskyPostable.identifier)
    val findSpacing = ( conn: Connection ) => MinBskyPostSpacing
    val attemptPost = attemptBskyPost
    val findNumRetries = ( conn : Connection ) => Config.blueskyMaxRetries( conn )
    val serviceName = "BlueSky"
    _parallelAcrossSpacedWithinAccountsNotifyAllPosts(
      fetchAllPostables,
      accountDiscriminator,
      findSpacing,
      attemptPost,
      findNumRetries,
      serviceName,
      ds,
      appSetup
    )


