package com.mchange.feedletter

import munit.ScalaCheckSuite
import org.scalacheck.Prop.forAll

// Destination CSV output is export-only (there is no parser in feedletter),
// so we verify that what we emit is well-formed, strict RFC-4180 CSV from which
// an ordinary CSV reader would recover the original values.
class DestinationCsvSuite extends ScalaCheckSuite:

  // strict single-record RFC-4180 parser. throws on malformed input.
  private def parseCsvRecord( record : String ) : Vector[String] =
    def fail( msg : String ) : Nothing = throw new AssertionError( s"Malformed CSV record ($msg): $record" )
    val FieldStart = 0; val Unquoted = 1; val Quoted = 2; val QuoteInQuoted = 3
    val fields = Vector.newBuilder[String]
    val sb = new java.lang.StringBuilder
    var state = FieldStart
    var i = 0
    while i < record.length do
      val c = record.charAt(i)
      state match
        case FieldStart =>
          c match
            case '"'         => state = Quoted
            case ','         => fields += ""
            case '\r' | '\n' => fail("record separator outside quoted field")
            case _           => sb.append(c); state = Unquoted
        case Unquoted =>
          c match
            case '"'         => fail("quote within unquoted field")
            case ','         => fields += sb.toString; sb.setLength(0); state = FieldStart
            case '\r' | '\n' => fail("record separator outside quoted field")
            case _           => sb.append(c)
        case Quoted =>
          c match
            case '"' => state = QuoteInQuoted
            case _   => sb.append(c)
        case QuoteInQuoted =>
          c match
            case '"' => sb.append('"'); state = Quoted // escaped quote
            case ',' => fields += sb.toString; sb.setLength(0); state = FieldStart
            case _   => fail("unexpected character after closing quote")
      i += 1
    state match
      case FieldStart    => fields += ""
      case Unquoted      => fields += sb.toString
      case Quoted        => fail("unterminated quoted field")
      case QuoteInQuoted => fields += sb.toString
    fields.result()

  // the raw values a CSV reader should recover from a destination's row
  private def logicalValues( d : Destination ) : Vector[String] =
    d match
      case e : Destination.Email    => Vector( e.addressPart, e.displayNamePart.getOrElse("") )
      case m : Destination.Mastodon => Vector( m.instanceUrl, m.name )
      case s : Destination.Sms      => Vector( s.number )
      case b : Destination.BlueSky  => Vector( b.entrywayUrl, b.identifier )

  private def record( d : Destination ) : String = d.toCsvRow.mkString(",")

  test("csv row headers parse and match row arity, for each destination type"):
    def check[T <: Destination : Destination.TypeMetaInfo]( sample : T ) : Unit =
      val headers = parseCsvRecord( Destination.csvRowHeaders[T].mkString(",") )
      assertEquals( headers.length, sample.toCsvRow.length, s"header/row arity mismatch for ${sample.getClass.getSimpleName}" )
    check( Destination.Email( "user@example.com", Some("Some User") ) )
    check( Destination.Mastodon( name = "mothership", instanceUrl = "https://mastodon.social/" ) )
    check( Destination.Sms( "+15555555555" ) )
    check( Destination.BlueSky( entrywayUrl = "https://bsky.social/", identifier = "interfluidity.com" ) )

  property("plausible destinations emit strict CSV rows that recover their values"):
    forAll( Gens.plausible.genDestination ): d =>
      assertEquals( parseCsvRecord( record(d) ), logicalValues(d) )

  property("arbitrary destinations emit strict CSV rows that recover their values"):
    forAll( Gens.hostile.genDestination ): d =>
      assertEquals( parseCsvRecord( record(d) ), logicalValues(d) )
