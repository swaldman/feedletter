package com.mchange.feedletter

import com.mchange.mailutil.*

import com.github.plokhotnyuk.jsoniter_scala.macros.*
import com.github.plokhotnyuk.jsoniter_scala.core.*

import java.nio.charset.Charset

object Jsoniter:
    given JsonValueCodec[Charset] with
      def nullValue: Charset = null
      def decodeValue(in : JsonReader, default : Charset): Charset = Charset.forName( in.readKeyAsString() )
      def encodeValue(cs : Charset, out: JsonWriter): Unit = out.writeKey( cs.name() )

    given addressCodec : JsonValueCodec[Smtp.Address] = JsonCodecMaker.make

