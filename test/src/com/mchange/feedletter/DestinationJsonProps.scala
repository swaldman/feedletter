package com.mchange.feedletter

import munit.ScalaCheckSuite
import org.scalacheck.Prop.forAll

// JSON pickling does no validation and JSON escaping should handle anything,
// so these round trips are expected to hold for completely arbitrary state.
class DestinationJsonProps extends ScalaCheckSuite:
  import Gens.hostile.*

  property("Destination.Email round trips through JSON"):
    forAll( genEmail ): d =>
      assertEquals( Destination.materialize( d.json ), d )

  property("Destination.Sms round trips through JSON"):
    forAll( genSms ): d =>
      assertEquals( Destination.materialize( d.json ), d )

  property("Destination.Mastodon round trips through JSON"):
    forAll( genMastodon ): d =>
      assertEquals( Destination.materialize( d.json ), d )

  property("Destination.BlueSky round trips through JSON"):
    forAll( genBlueSky ): d =>
      assertEquals( Destination.materialize( d.json ), d )

  property("any Destination round trips through pretty-printed JSON"):
    forAll( genDestination ): d =>
      assertEquals( Destination.materialize( d.jsonPretty ), d )
