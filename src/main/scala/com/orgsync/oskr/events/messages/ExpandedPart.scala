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

import java.time.{Duration, Instant}
import java.util.UUID

import com.orgsync.oskr.events.messages.parts._
import org.json4s.JsonAST.JArray
import org.json4s._
import org.threeten.extra.Interval

final case class ExpandedPart(
  id         : String,
  senderId   : String,
  recipient  : Recipient,
  sentAt     : Instant,
  groupingKey: Option[String],
  groupingGap: Option[Duration],
  digestKey  : String,
  tags       : Option[Set[String]],
  templates  : TemplateSet,
  data       : JValue
) {
  def toMessage: Message = {
    val idSource = recipient.id + id
    val messageId = UUID.nameUUIDFromBytes(idSource.getBytes)

    val sent = Interval.of(sentAt, sentAt)
    val messageTags = tags.getOrElse(Set[String]())

    Message(
      messageId, sentAt, Set(senderId), recipient, sent, digestKey, messageTags,
      templates, Set(id), JArray(List(data))
    )
  }
}
