package com.mchange.feedletter

import com.mchange.sc.v1.texttable
import zio.*

import java.time.format.DateTimeFormatter.ISO_INSTANT

val FeedInfoColumns = Seq(
  texttable.Column("Feed URL"),
  texttable.Column("Min Delay Minutes"),
  texttable.Column("Await Stabilization Minutes"),
  texttable.Column("Max Delay Minutes"),
  texttable.Column("Added"),
  texttable.Column("Last Assigned")
)

def printFeedInfoTable( fis: Set[FeedInfo] ) : Task[Unit] =
  ZIO.attempt( texttable.printProductTable( FeedInfoColumns )( fis.toList.map( texttable.Row.apply ) ) ) // preserve the order if the set is sorted

val ConfigKeyColumns = Seq( texttable.Column("Configuration Key"), texttable.Column("Value") )

def printConfigurationTuplesTable( tups : Set[Tuple2[ConfigKey,String]] ) : Task[Unit] =
  ZIO.attempt( texttable.printProductTable( ConfigKeyColumns )( tups.toList.map( texttable.Row.apply ) ) ) // preserve the order if the set is sorted

val ExcludedItemsColumns = Seq(
  texttable.Column("Feed URL"),
  texttable.Column("GUID"),
  texttable.Column("Title"),
  texttable.Column("Author"),
  texttable.Column("Pub Date")
)

def extractExcludedItem( ei : ExcludedItem ) : Seq[String] =
  ei.feedUrl.toString() :: ei.guid.toString() :: ei.title.getOrElse("") :: ei.author.getOrElse("") :: ei.publicationDate.map( ISO_INSTANT.format ).getOrElse("") :: Nil

def printExcludedItemsTable( eis : Set[ExcludedItem] ) : Task[Unit] =
  ZIO.attempt( texttable.printTable( ExcludedItemsColumns, extractExcludedItem )( eis.map(texttable. Row.apply) ) )

val SubscribableColumns = Seq(
  texttable.Column("Feed URL"),
  texttable.Column("Name"),
  texttable.Column("Subscription Type")
)

def printSubscribablesTable( tups : Set[(FeedUrl,SubscribableName,SubscriptionType)] ) : Task[Unit] =
  ZIO.attempt( texttable.printProductTable( SubscribableColumns )( tups.toList.map( texttable.Row.apply ) ) ) // preserve the order if the set is sorted



