package com.mchange.feedletter.db

import zio.*
import java.sql.*
import java.time.Instant
import javax.sql.DataSource
import scala.util.control.NonFatal
import java.lang.System

import com.mchange.feedletter.SubscriptionType

final case class ItemStatus( contentHash : Int, firstSeen : Instant, lastChecked : Instant, stableSince : Instant, assigned : Boolean )
final case class AssignableWithinTypeInfo( withinTypeId : String, count : Int )
final case class AssignableKey( feedUrl : String, stype : SubscriptionType, withinTypeId : String )
final case class FeedInfo( feedUrl : String, minDelaySeconds : Int, awaitStabilizationSeconds : Int, paused : Boolean )

enum MetadataKey:
  case SchemaVersion
  case CreatorAppVersion

def acquireConnection( ds : DataSource ) : Task[Connection] = ZIO.attemptBlocking( ds.getConnection )

def releaseConnection( conn : Connection ) : UIO[Unit] = ZIO.succeed:
  try
    conn.close()
  catch
    case NonFatal(t) =>
      System.err.println("Best-attempt close() of Connection yielded a throwable!")
      t.printStackTrace()

def withConnection[T]( ds : DataSource )( operation : Connection => T ) : Task[T] =
  withConnectionZIO( ds )( conn => ZIO.attemptBlocking( operation(conn) ) )

def withConnectionZIO[T]( ds : DataSource )( operation : Connection => Task[T]) : Task[T] =
  ZIO.acquireReleaseWith(acquireConnection(ds))( releaseConnection )( operation )

private def _inTransactionZIO[T]( conn : Connection )( transactioningHappyPath : Connection => Task[T]) : Task[T] =
  val rollback : PartialFunction[Throwable,Task[T]] =
    case NonFatal(t) =>
      ZIO.attemptBlocking( conn.rollback() ) *> ZIO.fail(t)
  val unsetAutocommit = ZIO.attemptBlocking( conn.setAutoCommit(false) ).logError.catchAll( _ => ZIO.unit )
  transactioningHappyPath(conn).catchSome( rollback ).ensuring( unsetAutocommit )

def inTransaction[T]( conn : Connection )( op : Connection => T) : Task[T] =
  val transactioningHappyPath = (cxn : Connection) => ZIO.attemptBlocking:
    cxn.setAutoCommit(true)
    val out = op(cxn)
    cxn.commit()
    out
  _inTransactionZIO(conn)(transactioningHappyPath)

def withConnectionTransactional[T]( ds : DataSource )( op : Connection => T) : Task[T] =
  withConnectionZIO(ds)( conn => inTransaction(conn)( op ) )

def inTransactionZIO[T]( conn : Connection )( op : Connection => Task[T]) : Task[T] =
  val transactioningHappyPath = (cxn : Connection ) =>
    for
      _ <- ZIO.attemptBlocking( cxn.setAutoCommit(true) )
      out <- op(cxn)
      _ <- ZIO.attemptBlocking( cxn.commit() )
    yield out
  _inTransactionZIO(conn)(transactioningHappyPath)

def withConnectionTransactionalZIO[T]( ds : DataSource )( op : Connection => Task[T] ) : Task[T] =
  withConnectionZIO(ds)( conn => inTransactionZIO(conn)( op ) )

def uniqueResult[T]( queryDesc : String, rs : ResultSet )( materialize : ResultSet => T ) : T =
  if !rs.next() then
    throw new UnexpectedlyEmptyResultSet("Expected a value for ${queryDesc}, none found.")
  else
    val out = materialize(rs)
    if rs.next() then
      throw new NonUniqueRow("Expected a unique value for ${queryDesc}. Multiple rows found.")
    else
      out

def zeroOrOneResult[T]( queryDesc : String, rs : ResultSet )( materialize : ResultSet => T ) : Option[T] =
  if !rs.next() then
    None
  else
    val out = materialize(rs)
    if rs.next() then
      throw new NonUniqueRow("Expected a unique value for ${queryDesc}. Multiple rows found.")
    else
      Some(out)

def setStringOptional( ps : PreparedStatement, position : Int, sqlType : Int, value : Option[String] ) =
  value match
    case Some( s ) => ps.setString(position, s)
    case None      => ps.setNull( position, sqlType )

def setTimestampOptional( ps : PreparedStatement, position : Int, value : Option[Timestamp] ) =
  value match
    case Some( ts ) => ps.setTimestamp(position, ts)
    case None       => ps.setNull( position, Types.TIMESTAMP )
