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

package com.orgsync.oskr.events.messages

import java.time.Instant

import com.orgsync.oskr.events.messages.parts.{ChannelAddress, Recipient}
import org.json4s.JValue

case class Delivery(
  id       : String,
  senderId : String,
  recipient: Recipient,
  address  : ChannelAddress,
  sentAt   : Instant,
  tags     : Option[Array[String]],
  broadcast: Option[Boolean],
  content  : JValue
)
