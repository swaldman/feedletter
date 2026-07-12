package com.mchange.feedletter

import java.time.ZoneId

// Round-trip properties can't catch a refactor that changes the wire format symmetrically.
// Since Destination and SubscriptionManager JSON is persisted in Postgres JSONB columns,
// the serialized form itself is a compatibility contract. These fixtures pin it:
// each golden string must materialize to the expected object, and the expected object
// must serialize to JSON structurally equal to the golden string.
class JsonGoldenSuite extends munit.FunSuite:

  private def assertDestinationGolden( golden : String, expected : Destination ) : Unit =
    assertEquals( Destination.materialize( Destination.Json( golden ) ), expected )
    assertEquals( ujson.read( expected.json.toString ), ujson.read( golden ) )

  private def assertSubscriptionManagerGolden( golden : String, expected : SubscriptionManager ) : Unit =
    assertEquals( SubscriptionManager.materialize( SubscriptionManager.Json( golden ) ), expected )
    assertEquals( ujson.read( expected.json.toString ), ujson.read( golden ) )

  test("Destination.Email golden fixtures"):
    assertDestinationGolden(
      """{"version":1,"type":"Email","addressPart":"user@example.com","displayNamePart":"Some User"}""",
      Destination.Email( "user@example.com", Some("Some User") )
    )
    assertDestinationGolden(
      """{"version":1,"type":"Email","addressPart":"user@example.com"}""",
      Destination.Email( "user@example.com", None )
    )

  test("Destination.Sms golden fixture"):
    assertDestinationGolden(
      """{"version":1,"type":"Sms","number":"+15555555555"}""",
      Destination.Sms( "+15555555555" )
    )

  test("Destination.Mastodon golden fixture"):
    assertDestinationGolden(
      """{"version":1,"type":"Mastodon","name":"mothership","instanceUrl":"https://mastodon.social/"}""",
      Destination.Mastodon( name = "mothership", instanceUrl = "https://mastodon.social/" )
    )

  test("Destination.BlueSky golden fixture"):
    assertDestinationGolden(
      """{"version":1,"type":"BlueSky","entrywayUrl":"https://bsky.social/","identifier":"interfluidity.com"}""",
      Destination.BlueSky( entrywayUrl = "https://bsky.social/", identifier = "interfluidity.com" )
    )

  test("SubscriptionManager.Email.Each golden fixture"):
    assertSubscriptionManagerGolden(
      """{
        |  "version" : 1,
        |  "type" : "Email.Each",
        |  "from" : {"version":1,"type":"Email","addressPart":"newsletter@example.com","displayNamePart":"Example Newsletter"},
        |  "composeUntemplateName" : "com.example.Compose",
        |  "confirmUntemplateName" : "com.example.Confirm",
        |  "statusChangeUntemplateName" : "com.example.StatusChange",
        |  "removalNotificationUntemplateName" : "com.example.RemovalNotification",
        |  "extraParams" : {"a":"b"},
        |  "replyTo" : {"version":1,"type":"Email","addressPart":"replies@example.com"}
        |}""".stripMargin,
      SubscriptionManager.Email.Each(
        from                              = Destination.Email( "newsletter@example.com", Some("Example Newsletter") ),
        replyTo                           = Some( Destination.Email( "replies@example.com", None ) ),
        composeUntemplateName             = "com.example.Compose",
        confirmUntemplateName             = "com.example.Confirm",
        statusChangeUntemplateName        = "com.example.StatusChange",
        removalNotificationUntemplateName = "com.example.RemovalNotification",
        extraParams                       = Map( "a" -> "b" )
      )
    )

  test("SubscriptionManager.Email.Daily golden fixture (no timeZone, no replyTo)"):
    assertSubscriptionManagerGolden(
      """{
        |  "version" : 1,
        |  "type" : "Email.Daily",
        |  "from" : {"version":1,"type":"Email","addressPart":"newsletter@example.com"},
        |  "composeUntemplateName" : "com.example.Compose",
        |  "confirmUntemplateName" : "com.example.Confirm",
        |  "statusChangeUntemplateName" : "com.example.StatusChange",
        |  "removalNotificationUntemplateName" : "com.example.RemovalNotification",
        |  "extraParams" : {}
        |}""".stripMargin,
      SubscriptionManager.Email.Daily(
        from                              = Destination.Email( "newsletter@example.com", None ),
        replyTo                           = None,
        composeUntemplateName             = "com.example.Compose",
        confirmUntemplateName             = "com.example.Confirm",
        statusChangeUntemplateName        = "com.example.StatusChange",
        removalNotificationUntemplateName = "com.example.RemovalNotification",
        timeZone                          = None,
        extraParams                       = Map.empty
      )
    )

  test("SubscriptionManager.Email.Weekly golden fixture (with timeZone)"):
    assertSubscriptionManagerGolden(
      """{
        |  "version" : 1,
        |  "type" : "Email.Weekly",
        |  "from" : {"version":1,"type":"Email","addressPart":"newsletter@example.com"},
        |  "composeUntemplateName" : "com.example.Compose",
        |  "confirmUntemplateName" : "com.example.Confirm",
        |  "statusChangeUntemplateName" : "com.example.StatusChange",
        |  "removalNotificationUntemplateName" : "com.example.RemovalNotification",
        |  "timeZone" : "America/New_York",
        |  "extraParams" : {}
        |}""".stripMargin,
      SubscriptionManager.Email.Weekly(
        from                              = Destination.Email( "newsletter@example.com", None ),
        replyTo                           = None,
        composeUntemplateName             = "com.example.Compose",
        confirmUntemplateName             = "com.example.Confirm",
        statusChangeUntemplateName        = "com.example.StatusChange",
        removalNotificationUntemplateName = "com.example.RemovalNotification",
        timeZone                          = Some( ZoneId.of("America/New_York") ),
        extraParams                       = Map.empty
      )
    )

  test("SubscriptionManager.Email.Fixed golden fixture"):
    assertSubscriptionManagerGolden(
      """{
        |  "version" : 1,
        |  "type" : "Email.Fixed",
        |  "from" : {"version":1,"type":"Email","addressPart":"newsletter@example.com"},
        |  "composeUntemplateName" : "com.example.Compose",
        |  "confirmUntemplateName" : "com.example.Confirm",
        |  "statusChangeUntemplateName" : "com.example.StatusChange",
        |  "removalNotificationUntemplateName" : "com.example.RemovalNotification",
        |  "numItemsPerLetter" : 5,
        |  "extraParams" : {}
        |}""".stripMargin,
      SubscriptionManager.Email.Fixed(
        from                              = Destination.Email( "newsletter@example.com", None ),
        replyTo                           = None,
        composeUntemplateName             = "com.example.Compose",
        confirmUntemplateName             = "com.example.Confirm",
        statusChangeUntemplateName        = "com.example.StatusChange",
        removalNotificationUntemplateName = "com.example.RemovalNotification",
        numItemsPerLetter                 = 5,
        extraParams                       = Map.empty
      )
    )

  test("SubscriptionManager.Mastodon.Announce golden fixture"):
    assertSubscriptionManagerGolden(
      """{"version":1,"type":"Mastodon.Announce","extraParams":{"k":"v"}}""",
      SubscriptionManager.Mastodon.Announce( extraParams = Map( "k" -> "v" ) )
    )

  test("SubscriptionManager.BlueSky.Announce golden fixture"):
    assertSubscriptionManagerGolden(
      """{"version":1,"type":"BlueSky.Announce","extraParams":{}}""",
      SubscriptionManager.BlueSky.Announce( extraParams = Map.empty )
    )
