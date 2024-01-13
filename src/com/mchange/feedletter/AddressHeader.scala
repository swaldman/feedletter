package com.mchange.feedletter

import com.mchange.mailutil.Smtp

sealed trait AddressHeaderType
opaque type To      <: AddressHeaderType = Nothing
opaque type From    <: AddressHeaderType = Nothing
opaque type ReplyTo <: AddressHeaderType = Nothing

object AddressHeader:
  def apply[T <: AddressHeaderType]( s : String )                      : AddressHeader[T] = s
  def apply[T <: AddressHeaderType]( address : Smtp.Address )          : AddressHeader[T] = address.rendered
  def apply[T <: AddressHeaderType]( destination : Destination.Email ) : AddressHeader[T] = destination.rendered
opaque type AddressHeader[T <: AddressHeaderType] = String
