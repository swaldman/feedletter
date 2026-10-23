package com.mchange.feedletter

import scala.xml.XML

import com.mchange.feedletter.default.email.{embedMedia,isEmbeddableMedia}

// Mastodon transcodes animated GIFs to silent looping mp4 ("gifv"), and publishes them in RSS
// as type="video/mp4" but medium="image". These should be embedded, not dropped like real video.
class MediaSuite extends munit.FunSuite:

  private val Guid = "https://zirk.us/@interfluidity/117489679409447139"

  private val Rss =
    s"""<?xml version="1.0" encoding="UTF-8"?>
       |<rss version="2.0" xmlns:media="http://search.yahoo.com/mrss/">
       |  <channel>
       |    <title>interfluidity</title>
       |    <link>https://zirk.us/@interfluidity</link>
       |    <description>Public posts from @interfluidity@zirk.us</description>
       |    <item>
       |      <guid isPermaLink="true">${Guid}</guid>
       |      <link>${Guid}</link>
       |      <pubDate>Fri, 23 Oct 2026 10:10:35 +0000</pubDate>
       |      <description>&lt;p&gt;if you are going to cheerlead growth...&lt;/p&gt;</description>
       |      <media:content url="https://example.com/original/30c6abc16f3523e1.mp4" type="video/mp4" fileSize="63454" medium="image">
       |        <media:rating scheme="urn:simple">nonadult</media:rating>
       |        <media:description type="plain">The monster plant from "Little Shop of Horrors" shouting "Feed Me!"</media:description>
       |      </media:content>
       |      <media:content url="https://example.com/real-video.mp4" type="video/mp4" medium="video"/>
       |      <media:content url="https://example.com/still.png" type="image/png" medium="image"/>
       |    </item>
       |  </channel>
       |</rss>""".stripMargin

  private lazy val media = ItemContent.fromRssGuid( XML.loadString( Rss ), Guid ).media

  test("medium is captured from media:content"):
    assertEquals( media.map( _.medium ), Seq( Some("image"), Some("video"), Some("image") ) )

  test("animated image as video is embeddable, true video is not"):
    assertEquals( media.map( _.isAnimatedImageAsVideo ), Seq( true, false, false ) )
    assertEquals( media.map( isEmbeddableMedia ), Seq( true, false, true ) )

  test("animated image renders as muted looping video with visible linked caption"):
    val html = embedMedia( media.head, Some( Guid ) )
    assert( html.contains("""<video src="https://example.com/original/30c6abc16f3523e1.mp4" autoplay loop muted playsinline"""), html )
    assert( html.contains(s"""<a href="${Guid}">&#9654; Animated image: The monster plant from &quot;Little Shop of Horrors&quot;"""), html )

  test("caption falls back to the media url when the item has no link"):
    assert( embedMedia( media.head, None ).contains("""<a href="https://example.com/original/30c6abc16f3523e1.mp4">"""), embedMedia( media.head, None ) )

  test("still images render as before"):
    assertEquals( embedMedia( media(2), Some( Guid ) ), """<img src="https://example.com/still.png">""" )
