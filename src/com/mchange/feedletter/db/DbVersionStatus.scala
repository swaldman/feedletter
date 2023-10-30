package com.mchange.feedletter.db

object DbVersionStatus:
  case class Current( version : Int ) extends DbVersionStatus
  case class OutOfDate( schemaVersion : Int, requiredVersion : Int ) extends DbVersionStatus
  case class UnexpectedVersion( schemaVersion : Option[String], creatorAppVersion : Option[String], currentAppVersion : Option[String], latestKnownSchemaVersion : Option[String] ) extends DbVersionStatus
  case class SchemaMetadataDisordered( message : String ) extends DbVersionStatus
  case object SchemaMetadataNotFound extends DbVersionStatus
  case object ConnectionFailed extends DbVersionStatus
sealed trait DbVersionStatus:
  import DbVersionStatus.* 
  def errMessage : Option[String] =
    this match
      case Current( _ ) => None
      case OutOfDate(schemaVersion, requiredVersion) =>
        Some(
          s"""|The database is out of date. Its current version is ${schemaVersion}, but version ${requiredVersion} is required.
              |Please run 'feedletter db dump' to backup, then run 'feedletter db migrate'.
              |""".stripMargin
        )
      case UnexpectedVersion(schemaVersion, creatorAppVersion, currentAppVersion, latestKnownSchemaVersion) =>
        Some(
          s"""|Found database version ${schemaVersion.getOrElse("<no version found>")}, which is unknown to
              |and unexpected by current application version ${currentAppVersion.getOrElse("<unknown>")}.
              |The latest db version this version of the application expects to see is ${latestKnownSchemaVersion.getOrElse("<unknown>")}
              |The database was created or last migrated from application version ${creatorAppVersion.getOrElse("<unknown>")}.
              |""".stripMargin
        )
      case SchemaMetadataDisordered( message ) =>
        Some( message )
      case SchemaMetadataNotFound =>
        Some(s"""No schema metadata found. The database has not been initialized, or else it has been badly corrupted.""")
      case ConnectionFailed =>
        Some(s"""Failed to connect to the database when trying to examine the database version.""")
