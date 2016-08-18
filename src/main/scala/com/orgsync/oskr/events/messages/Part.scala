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

import com.orgsync.oskr.events.messages.parts.{Recipient, TemplateSet}
import org.json4s._

final case class Part(
  id         : String,
  senderId   : String,
  recipients : List[Recipient],
  sentAt     : Instant,
  groupingKey: Option[String],
  groupingGap: Option[Duration],
  tags       : Option[Set[String]],
  templates  : TemplateSet,
  data       : JValue
) {
  require(id != null)
  require(senderId != null)
  require(recipients != null)
  require(sentAt != null)
  require(templates != null)
  require(data != null)

  def toExpandedParts: List[ExpandedPart] = {
    recipients.map(r =>
      ExpandedPart(
        id, senderId, r, sentAt, groupingKey, groupingGap, tags, templates, data
      )
    )
  }
}
