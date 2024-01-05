def ifLink( link : Option[String], text : String ) : String =
  link.fold(text)(l => s"""<a href="$l">$text</a>""")
end ifLink

