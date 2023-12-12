package com.mchange.feedletter.trivialtemplate

object TrivialTemplate:
  private val KeyRegex        = """(?<!\\)\%(\w+)\%""".r
  private val EscapedKeyRegex = """\\+\%\w+\%""".r

  object Defaults:
    val Fail : String => String = k => throw new KeyNotFound(s"Key '$k' required by template, not found.")
    val AsIs : String => String = k => s"%$k%"

  def escapeText( text : String ) : String =
    KeyRegex.replaceAllIn( text, m => """\""" + m.group(0) )

case class TrivialTemplate( body : String ):
  import TrivialTemplate.*

  def resolve( replacements : Map[String,String], defaults : String => String = Defaults.Fail ) : String =
    val replaced = KeyRegex.replaceAllIn( body, m => { val k = m.group(1); replacements.getOrElse( k, defaults(k) ) } )
    val unescaped = EscapedKeyRegex.replaceAllIn( replaced, m => m.group(0).substring(1) ) 
    unescaped

