package com.mchange.feedletter.db

import com.mchange.feedletter.*

import zio.*
import java.sql.*
import java.time.Instant
import javax.sql.DataSource
import scala.util.control.NonFatal
import java.lang.System

import com.mchange.cryptoutil.*

enum MetadataKey:
  case SchemaVersion
  case CreatorAppVersion

object MailSpec:
  final case class WithHash(
    seqnum       : Long,
    templateHash : Hash.SHA3_256,
    from         : AddressHeader[From],
    replyTo      : Option[AddressHeader[ReplyTo]],
    to           : AddressHeader[To],
    subject : String,
    templateParams : TemplateParams,
    retried : Int
  )
  final case class WithTemplate(
    seqnum : Long,
    templateHash : Hash.SHA3_256,
    template : String,
    from : AddressHeader[From],
    replyTo : Option[AddressHeader[ReplyTo]],
    to : AddressHeader[To],
    subject : String,
    templateParams : TemplateParams,
    retried : Int
  )

def acquireConnection( ds : DataSource ) : Task[Connection] = ZIO.attemptBlocking( ds.getConnection )

private def _inTransactionZIO[T]( conn : Connection )( transactioningHappyPath : Connection => Task[T]) : Task[T] =
  val rollback : PartialFunction[Throwable,Task[T]] =
    case NonFatal(t) =>
      ZIO.attemptBlocking( conn.rollback() ) *> ZIO.fail(t)
  val resetAutocommit = ZIO.attemptBlocking( conn.setAutoCommit(true) ).logError.catchAll( _ => ZIO.unit )
  transactioningHappyPath(conn).catchSome( rollback ).ensuring( resetAutocommit )

def inTransaction[T]( conn : Connection )( op : Connection => T) : Task[T] =
  val transactioningHappyPath = (cxn : Connection) => ZIO.attemptBlocking:
    cxn.setAutoCommit(false)
    val out = op(cxn)
    cxn.commit()
    out
  _inTransactionZIO(conn)(transactioningHappyPath)

def inTransactionZIO[T]( conn : Connection )( op : Connection => Task[T]) : Task[T] =
  val transactioningHappyPath = (cxn : Connection ) =>
    for
      _ <- ZIO.attemptBlocking( cxn.setAutoCommit(false) )
      out <- op(cxn)
      _ <- ZIO.attemptBlocking( cxn.commit() )
    yield out
  _inTransactionZIO(conn)(transactioningHappyPath)

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

def withConnectionTransactional[T]( ds : DataSource )( op : Connection => T) : Task[T] =
  withConnectionZIO(ds)( conn => inTransaction(conn)( op ) )

def withConnectionTransactionalZIO[T]( ds : DataSource )( op : Connection => Task[T] ) : Task[T] =
  withConnectionZIO(ds)( conn => inTransactionZIO(conn)( op ) )


