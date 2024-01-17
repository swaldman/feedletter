package com.mchange.feedletter

import com.mchange.feedletter.style.untemplateInputType

import com.mchange.sc.v1.texttable
import zio.*

import untemplate.Untemplate

import scala.collection.immutable

import java.time.Instant
import java.time.format.DateTimeFormatter.ISO_INSTANT

val FeedInfoColumns = Seq(
  texttable.Column("Feed ID"),
  texttable.Column("Feed URL"),
  texttable.Column("Min Delay Mins"),
  texttable.Column("Await Stabilization Mins"),
  texttable.Column("Max Delay Mins"),
  texttable.Column("Recheck Every Mins"),
  texttable.Column("Added"),
  texttable.Column("Last Assigned")
)

def printFeedInfoTable( fis: Set[FeedInfo] ) : Task[Unit] =
  val ordering = Ordering.by( (fi : FeedInfo) => ( fi.feedId.toInt, fi.feedUrl.str, fi.minDelayMinutes, fi.awaitStabilizationMinutes, fi.maxDelayMinutes, fi.assignEveryMinutes, fi.added, fi.lastAssigned ) )
  val sorted = immutable.SortedSet.from( fis )( using ordering )
  ZIO.attempt( texttable.printProductTable( FeedInfoColumns )( sorted.toList.map( texttable.Row.apply ) ) )

val ConfigKeyColumns = Seq( texttable.Column("Configuration Key"), texttable.Column("Value") )

def printConfigurationTuplesTable( tups : Set[Tuple2[ConfigKey,String]] ) : Task[Unit] =
  ZIO.attempt( texttable.printProductTable( ConfigKeyColumns )( tups.toList.map( texttable.Row.apply ) ) ) // preserve the order if the set is sorted

val ExcludedItemsColumns = Seq(
  texttable.Column("Feed ID"),
  texttable.Column("GUID"),
  texttable.Column("Link")
)

def extractExcludedItem( ei : ExcludedItem ) : Seq[String] =
  ei.feedId.toString :: ei.guid.str :: ei.link.getOrElse("") :: Nil

def printExcludedItemsTable( eis : Set[ExcludedItem] ) : Task[Unit] =
  ZIO.attempt( texttable.printTable( ExcludedItemsColumns, extractExcludedItem )( eis.map(texttable. Row.apply) ) )

val UntemplatesColumns = Seq(
  texttable.Column("Untemplate, Fully Qualified Name"),
  texttable.Column("Input Type")
)

def printUntemplatesTable( tups : Iterable[(String,Untemplate.AnyUntemplate)] ) : Task[Unit] =
  val sorted = immutable.SortedMap.from( tups ).toList
  ZIO.attempt( texttable.printProductTable( UntemplatesColumns )( sorted.map((k,v)=>(k,untemplateInputType(v))).map( texttable.Row.apply ) ) )

val SubscriptionColumns = Seq(
  texttable.Column("Subscription ID"),
  texttable.Column("Subscriber"),
  texttable.Column("Confirmed"),
  texttable.Column("Added")
)

def printSubscriptions( tups : Iterable[(SubscriptionId,String,Boolean,Instant)] ) =
  ZIO.attempt( texttable.printProductTable( SubscriptionColumns )( tups.map( texttable.Row.apply ) ) )


