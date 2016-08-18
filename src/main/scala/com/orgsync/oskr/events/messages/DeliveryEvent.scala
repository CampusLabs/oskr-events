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
import java.util.UUID

import com.orgsync.oskr.events.messages.delivery_events.DeliveryEventType
import com.orgsync.oskr.events.messages.parts.ChannelType
import org.json4s.JsonAST.JValue

final case class DeliveryEvent(
  deliverableId: UUID,
  recipientId  : String,
  channel      : ChannelType,
  action       : DeliveryEventType,
  at           : Instant,
  data         : Option[JValue]
) {
  require(deliverableId != null)
  require(recipientId != null)
  require(channel != null)
  require(action != null)
  require(at != null)
}
