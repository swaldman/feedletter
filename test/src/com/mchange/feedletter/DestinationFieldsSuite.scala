package com.mchange.feedletter

import munit.ScalaCheckSuite
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

// The fields representation, unlike JSON, has contracts:
//   * blank (trimmed-empty) keys and values are silently dropped by fromFields
//   * an Email without a display name is re-parsed via Smtp.Address.parseSingle on the way back
// so round-trip properties here use non-blank values, and plausible addresses where parsing is involved.
class DestinationFieldsSuite extends ScalaCheckSuite:
  import Gens.genNonBlankString

  private val genEmailWithDisplayName : Gen[Destination.Email] =
    for
      addressPart     <- genNonBlankString
      displayNamePart <- genNonBlankString
    yield
      Destination.Email( addressPart, Some( displayNamePart ) )
  private val genEmailPlainAddress : Gen[Destination.Email] = Gens.plausible.genEmailAddress.map( ap => Destination.Email( ap, None ) )
  private val genMastodonNonBlank : Gen[Destination.Mastodon] =
    for
      name        <- genNonBlankString
      instanceUrl <- genNonBlankString
    yield
      Destination.Mastodon( name, instanceUrl )
  private val genSmsNonBlank : Gen[Destination.Sms] = genNonBlankString.map( Destination.Sms.apply )
  private val genBlueSkyNonBlank : Gen[Destination.BlueSky] =
    for
      entrywayUrl <- genNonBlankString
      identifier  <- genNonBlankString
    yield
      Destination.BlueSky( entrywayUrl, identifier )

  private def untyped( fields : Seq[(String,String)] ) : Seq[(String,String)] = fields.filterNot( _(0) == "destinationType" )

  property("Email with display name round trips through fields (no address parsing involved)"):
    forAll( genEmailWithDisplayName ): d =>
      assertEquals( Destination.fromFields( d.toFields ), Some( d ) )

  property("Email without display name round trips through fields, for plausible addresses"):
    forAll( genEmailPlainAddress ): d =>
      assertEquals( Destination.fromFields( d.toFields ), Some( d ) )

  property("Mastodon round trips through fields"):
    forAll( genMastodonNonBlank ): d =>
      assertEquals( Destination.fromFields( d.toFields ), Some( d ) )

  property("Sms round trips through fields"):
    forAll( genSmsNonBlank ): d =>
      assertEquals( Destination.fromFields( d.toFields ), Some( d ) )

  property("BlueSky round trips through fields"):
    forAll( genBlueSkyNonBlank ): d =>
      assertEquals( Destination.fromFields( d.toFields ), Some( d ) )

  property("fields without a type tag resolve unambiguously for Mastodon"):
    forAll( genMastodonNonBlank ): d =>
      assertEquals( Destination.fromFields( untyped( d.toFields ) ), Some( d ) )

  property("fields without a type tag resolve unambiguously for Sms"):
    forAll( genSmsNonBlank ): d =>
      assertEquals( Destination.fromFields( untyped( d.toFields ) ), Some( d ) )

  property("fields without a type tag resolve unambiguously for Email with display name"):
    forAll( genEmailWithDisplayName ): d =>
      assertEquals( Destination.fromFields( untyped( d.toFields ) ), Some( d ) )

  property("Email accepts friendlier 'address'/'displayName' field keys"):
    forAll( genEmailWithDisplayName ): d =>
      val fields = Seq( "destinationType" -> "Email", "address" -> d.addressPart, "displayName" -> d.displayNamePart.get )
      assertEquals( Destination.fromFields( fields ), Some( d ) )

  test("fields with duplicate keys and conflicting values provoke DuplicateKey"):
    intercept[DuplicateKey]:
      Destination.fromFields( Seq( "destinationType" -> "Sms", "number" -> "555-1212", "number" -> "555-2121" ) )

  test("untagged fields interpretable as multiple destination types provoke AmbiguousDestination"):
    intercept[AmbiguousDestination]:
      Destination.fromFields( Seq( "addressPart" -> "user@example.com", "name" -> "mothership", "instanceUrl" -> "https://mastodon.social/" ) )

  test("blank values are dropped, potentially leaving fields unresolvable"):
    assertEquals( Destination.fromFields( Seq( "destinationType" -> "Sms", "number" -> "   " ) ), None )

  test("destination type tags are matched case-insensitively"):
    assertEquals( Destination.fromFields( Seq( "destinationType" -> "sms", "number" -> "555-1212" ) ), Some( Destination.Sms( "555-1212" ) ) )
    assertEquals( Destination.fromFields( Seq( "type" -> "EMAIL", "addressPart" -> "user@example.com" ) ), Some( Destination.Email( "user@example.com", None ) ) )

  test("unknown destination type tags yield None"):
    assertEquals( Destination.fromFields( Seq( "destinationType" -> "carrierPigeon", "number" -> "555-1212" ) ), None )
