package com.mchange.feedletter

import zio.*
import com.mchange.feedletter.db.PgDatabase
import javax.sql.DataSource

import MLevel.*

object Daemon extends SelfLogging:

  private object RetrySchedule:
    val updateAssignComplete = Schedule.exponential( 10.seconds, 1.25f ) && Schedule.upTo( 5.minutes ) // XXX: hard-coded for now
    val mailNextGroupIfDue = updateAssignComplete // XXX: for now!

  private object CyclingSchedule:
    val updateAssignComplete = Schedule.fixed( 1.minute ).jittered(0.0, 0.5) // XXX: hard-coded for now
    val mailNextGroupIfDue = updateAssignComplete // XXX: for now!

  // updateAssign and complete are distinct transactions,
  // and everything is idempotent.
  //
  // but completion work logically follows updateAssign, so for now at
  // least we are combining them.
  def updateAssignComplete( ds : DataSource ) : Task[Unit] =
    PgDatabase.updateAssignItems( ds ) *> PgDatabase.completeAssignables( ds )

  def retryingUpdateAssignComplete( ds : DataSource ) : Task[Unit] =
    updateAssignComplete( ds )
      .tapError( t => WARNING.zlog("updateAssignComplete failed. May retry.", t ) )
      .retry( RetrySchedule.updateAssignComplete )

  def cyclingRetryingUpdateAssignComplete( ds : DataSource ) : Task[Unit] =
    retryingUpdateAssignComplete( ds )
      .catchAll( t => WARNING.zlog( "Retry cycle for updateAssignComplete failed...", t ) )
      .schedule( CyclingSchedule.updateAssignComplete ) *> ZIO.unit

  def retryingMailNextGroupIfDue( ds : DataSource ) : Task[Unit] =
    PgDatabase.mailNextGroupIfDue( ds )
      .tapError( t => WARNING.zlog("mailNextGroupIfDue failed. May retry.", t ) )
      .retry( RetrySchedule.mailNextGroupIfDue )

  def cyclingRetryingMailNextGroupIfDue( ds : DataSource ) : Task[Unit] =
    retryingMailNextGroupIfDue( ds )
      .catchAll( t => WARNING.zlog( "Retry cycle for mailNextGroupIfDue failed...", t ) )
      .schedule( CyclingSchedule.mailNextGroupIfDue ) *> ZIO.unit

