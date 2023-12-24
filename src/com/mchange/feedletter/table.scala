package com.mchange.feedletter

import com.mchange.sc.v1.texttable
import zio.*

import untemplate.Untemplate

import java.time.format.DateTimeFormatter.ISO_INSTANT

val FeedInfoColumns = Seq(
  texttable.Column("Feed ID"),
  texttable.Column("Feed URL"),
  texttable.Column("Min Delay Minutes"),
  texttable.Column("Await Stabilization Minutes"),
  texttable.Column("Max Delay Minutes"),
  texttable.Column("Recheck Every Minutes"),
  texttable.Column("Added"),
  texttable.Column("Last Assigned")
)

def printFeedInfoTable( fis: Set[FeedInfo] ) : Task[Unit] =
  ZIO.attempt( texttable.printProductTable( FeedInfoColumns )( fis.toList.map( texttable.Row.apply ) ) ) // preserve the order if the set is sorted

val ConfigKeyColumns = Seq( texttable.Column("Configuration Key"), texttable.Column("Value") )

def printConfigurationTuplesTable( tups : Set[Tuple2[ConfigKey,String]] ) : Task[Unit] =
  ZIO.attempt( texttable.printProductTable( ConfigKeyColumns )( tups.toList.map( texttable.Row.apply ) ) ) // preserve the order if the set is sorted

val ExcludedItemsColumns = Seq(
  texttable.Column("Feed ID"),
  texttable.Column("GUID"),
  texttable.Column("Link")
)

def extractExcludedItem( ei : ExcludedItem ) : Seq[String] =
  ei.feedId.toString() :: ei.guid.toString() :: ei.link.getOrElse("") :: Nil

def printExcludedItemsTable( eis : Set[ExcludedItem] ) : Task[Unit] =
  ZIO.attempt( texttable.printTable( ExcludedItemsColumns, extractExcludedItem )( eis.map(texttable. Row.apply) ) )

val SubscribableColumns = Seq(
  texttable.Column("Name"),
  texttable.Column("Feed ID"),
  texttable.Column("Subscription Manager")
)

def printSubscribablesTable( tups : Set[(SubscribableName,FeedId,SubscriptionManager)] ) : Task[Unit] =
  ZIO.attempt( texttable.printProductTable( SubscribableColumns )( tups.toList.map( texttable.Row.apply ) ) ) // preserve the order if the set is sorted

val UntemplatesColumns = Seq(
  texttable.Column("Untemplate, Fully Qualified Name"),
  texttable.Column("Input Type")
)

def printUntemplatesTable( tups : Iterable[(String,Untemplate.AnyUntemplate)] ) : Task[Unit] =
  ZIO.attempt( texttable.printProductTable( UntemplatesColumns )( tups.map((k,v)=>(k,untemplateInputType(v))).map( texttable.Row.apply ) ) )

