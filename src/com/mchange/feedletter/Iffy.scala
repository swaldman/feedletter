package com.mchange.feedletter

import scala.xml.Elem
import audiofluidity.rss.{Element,Namespace}
import audiofluidity.rss.util.*

import scala.util.control.NonFatal

import com.mchange.conveniences.collection.*

import MLevel.*

object Iffy extends SelfLogging:
  private def iffyElem( elem : Elem ) : Boolean = elem.prefix == "iffy" || scopeContainsNamespaceLenient(Namespace.Iffy.unprefixed, elem.scope)
  object HintAnnounce:
    val  Policy = Element.Iffy.HintAnnounce.Policy
    type Policy = Element.Iffy.HintAnnounce.Policy

    def extract( itemElem : Elem ) : Seq[HintAnnounce] =
      val ihas = (itemElem \ "hint-announce").asInstanceOf[Seq[Elem]].filter( iffyElem )
      ihas.flatMap: n =>
        val mbPolicyElem = (n \ "policy").asInstanceOf[Seq[Elem]].filter( iffyElem ).uniqueOrNone
        mbPolicyElem match
          case Some( policyElem ) =>
            try
              val rawPolicy = policyElem.text.trim
              Policy.lenientParse( rawPolicy ) match
                case Some( policy ) =>
                  val restriction =
                    (n \ "restriction").asInstanceOf[Seq[Elem]].filter( iffyElem ).zeroOrOne match
                      case Right( opt ) => opt.flatMap( r => if r.child.isEmpty then None else Some(r) ) // treat empty restrictions as no restriction
                      case Left( n ) => throw new FeedletterException( "Expected one or zero restrictions in iffy:hint-announce, found " + n )
                  Some( Iffy.HintAnnounce( policy, restriction ) )
                case None =>
                  WARNING.log(s"Unknown policy '${rawPolicy}', illegal iffy:hint-announce ignored: " + n )
                  None
            catch
              case NonFatal(t) =>
                WARNING.log("Illegal iffy:hint-announce ignored: " + n, t )
                None
          case None =>
            WARNING.log("Illegal iffy:hint-announce ignored, missing policy element: " + n )
            None

  case class HintAnnounce( policy : HintAnnounce.Policy, restriction : Option[Elem] )
