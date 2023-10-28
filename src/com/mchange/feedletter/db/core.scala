package com.mchange.feedletter.db

import zio.*
import javax.sql.DataSource
import java.sql.Connection
import scala.util.control.NonFatal
import java.lang.System

def acquireConnection( ds : DataSource ) : Task[Connection] = ZIO.attemptBlocking( ds.getConnection )

def releaseConnection( conn : Connection ) : UIO[Unit] = ZIO.succeed:
  try
    conn.close()
  catch
    case NonFatal(t) =>
      System.err.println("Best-attempt close() of Connection yielded a throwable!")
      t.printStackTrace()
      
def withConnection[T]( ds : DataSource )( operation : Connection => Task[T]) : Task[T] =
  ZIO.acquireReleaseWith(acquireConnection(ds))( releaseConnection )( operation )
