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

final case class Part(
  id         : String,
  senderId   : String,
  recipient  : Recipient,
  channels   : List[ChannelAddress],
  sentAt     : Instant,
  groupingKey: Option[String],
  groupingGap: Option[Duration],
  tags       : Option[Set[String]],
  digestKey  : Option[String],
  digestAt   : Option[Instant],
  templates  : TemplateSet,
  data       : JValue
) {
  def toMessage: Message = {
    val idSource = id + recipient.id
    val messageId = UUID.nameUUIDFromBytes(idSource.getBytes)

    val sent = Interval.of(sentAt, sentAt)

    val messageTags = tags match {
      case Some(ts) => ts
      case None => Set[String]()
    }

    Message(
      messageId, Set(senderId), recipient, channels, sent, messageTags,
      digestKey, digestAt, templates, List(id), JArray(List(data))
    )
  }
}

object Parts {
  def toMessage(parts: Iterable[Part]): Option[Message] = {
    val partList = parts.toList

    val buf = new StringBuilder
    partList.foreach(p => {
      buf ++= p.id
      buf ++= p.recipient.id
    })

    val messageId = UUID.nameUUIDFromBytes(buf.toString.getBytes)

    val senderIds = partList.map(_.senderId).toSet
    val sendTimes = partList.map(_.sentAt).sorted
    val sendInterval = Interval.of(sendTimes.head, sendTimes.last)
    val tags = partList.flatMap(_.tags.getOrElse(Set[String]())).toSet

    partList
      .headOption
      .map(p => Message(
        messageId, senderIds, p.recipient, p.channels, sendInterval, tags,
        p.digestKey, p.digestAt, p.templates, partList.map(_.id),
        JArray(partList.map(_.data))
      ))
  }
}

