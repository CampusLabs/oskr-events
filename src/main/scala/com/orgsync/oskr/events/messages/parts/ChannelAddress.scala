/*
 * Copyright 2016 OrgSync.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orgsync.oskr.events.messages.parts

import java.time.Duration
import java.util.UUID

sealed trait ChannelAddress {
  def channel: ChannelType

  def address: String

  def delay: Duration

  def deliveryId: Option[UUID]
}

final case class StorageChannelAddress(
  address   : String,
  delay     : Duration,
  deliveryId: Option[UUID]
) extends ChannelAddress {
  val channel = Storage
}

final case class WebChannelAddress(
  address   : String,
  delay     : Duration,
  deliveryId: Option[UUID]
) extends ChannelAddress {
  val channel = Web
}

final case class PushChannelAddress(
  address   : String,
  delay     : Duration,
  deliveryId: Option[UUID]
) extends ChannelAddress {
  val channel = Push
}

final case class SMSChannelAddress(
  address   : String,
  delay     : Duration,
  deliveryId: Option[UUID]
) extends ChannelAddress {
  val channel = SMS
}

final case class EmailChannelAddress(
  address   : String,
  delay     : Duration,
  deliveryId: Option[UUID]
) extends ChannelAddress {
  val channel = Email
}


