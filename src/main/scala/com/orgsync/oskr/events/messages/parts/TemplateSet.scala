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

import com.orgsync.oskr.events.ChannelTemplateMap
import com.orgsync.oskr.events.messages.Message
import org.json4s.JsonAST._

final case class TemplateSet(
  base        : ChannelTemplateMap,
  digestBase  : ChannelTemplateMap,
  digestLayout: ChannelTemplateMap
) {
  def renderBase(
    address: ChannelAddress,
    message: Message
  ): Option[JValue] = renderChannelTemplate(
    base, address.channel, message.recipient, message.parts
  )

  def renderDigest(
    address : ChannelAddress,
    messages: List[Message]
  ): Option[JValue] = {
    val data = messages.flatMap(message => renderChannelTemplate(
      digestBase, address.channel, message.recipient, message.parts
    ))

    messages.headOption.flatMap(message => renderChannelTemplate(
      digestLayout, address.channel, message.recipient, JArray(data)
    ))
  }

  private def renderChannelTemplate(
    templates: ChannelTemplateMap,
    channel  : ChannelType,
    recipient: Recipient,
    data     : JArray
  ): Option[JValue] = {
    val template = templates.get(channel)
    val value = JObject(("recipient", recipient.data) :: ("data", data) :: Nil)

    template.map(t => renderTemplate(t, value))
  }

  private def renderTemplate(template: JValue, data: JObject): JValue = {
    template.transform {
      case JString(s) => renderTemplateString(s, data)
    }
  }

  private val TemplatePattern = """\Atemplate/([a-zA-Z0-9]+):(.*)\z""".r

  private def renderTemplateString(s: String, data: JObject): JString = {
    s match {
      case TemplatePattern(name, body) => name match {
        case "handlebars" => JString(body)
        case "none" => JString(body)
        case _ => JString(s)
      }
      case _ => JString(s)
    }
  }
}
