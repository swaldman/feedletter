package com.mchange.feedletter.trivialtemplate

object TrivialTemplate:
  private val KeyRegex        = """(?<!\\)\%(\w+)\%""".r
  private val EscapedKeyRegex = """\\+\%\w+\%""".r

  private val FailByDefault : String => String = k =>
    throw new KeyNotFound(s"Key '$k' required by template, not found.")

case class TrivialTemplate( body : String ):
  import TrivialTemplate.*
  
  def resolve( replacements : Map[String,String], defaults : String => String = FailByDefault ) : String =
    val replaced = KeyRegex.replaceAllIn( body, m => { val k = m.group(1); replacements.getOrElse( k, defaults(k) ) } )
    val unescaped = EscapedKeyRegex.replaceAllIn( replaced, m => m.group(0).substring(1) ) 
    unescaped

