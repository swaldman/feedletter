package com.mchange.feedletter

import munit.ScalaCheckSuite
import org.scalacheck.Prop.forAll

// SubscriptionManagers are persisted as JSONB in the subscribable table,
// so these round trips must hold for arbitrary state, including the
// omitted-when-None optional fields (replyTo, timeZone) and nested Destinations.
class SubscriptionManagerJsonProps extends ScalaCheckSuite:
  import Gens.subscriptionManager.*

  private def roundTrips( sm : SubscriptionManager ) : Unit =
    assertEquals( SubscriptionManager.materialize( sm.json ), sm )
    assertEquals( SubscriptionManager.materialize( sm.jsonPretty ), sm )

  property("Email.Each round trips through JSON"):
    forAll( genEmailEach )( roundTrips )

  property("Email.Daily round trips through JSON"):
    forAll( genEmailDaily )( roundTrips )

  property("Email.Weekly round trips through JSON"):
    forAll( genEmailWeekly )( roundTrips )

  property("Email.Fixed round trips through JSON"):
    forAll( genEmailFixed )( roundTrips )

  property("Mastodon.Announce round trips through JSON"):
    forAll( genMastodonAnnounce )( roundTrips )

  property("BlueSky.Announce round trips through JSON"):
    forAll( genBlueSkyAnnounce )( roundTrips )
