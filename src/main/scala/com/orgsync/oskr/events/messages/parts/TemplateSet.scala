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
import com.orgsync.oskr.events.streams.deliveries.TemplateCache
import org.json4s.JsonAST._

import scala.util.Try

final case class TemplateSet(
  base        : ChannelTemplateMap,
  digestBase  : ChannelTemplateMap,
  digestLayout: ChannelTemplateMap
) {
  def renderBase(
    address: ChannelAddress,
    message: Message,
    cache  : TemplateCache
  ): Option[JValue] = renderBaseTemplate(
    base, address, message, message.partData, cache
  )

  def renderDigest(
    address : ChannelAddress,
    messages: List[Message],
    cache   : TemplateCache
  ): Option[JValue] = {
    val data = messages.flatMap(message => renderBaseTemplate(
      digestBase, address, message, message.partData, cache
    ))

    val partIds = messages.flatMap(_.partIds)

    messages.headOption.flatMap(message => renderLayoutTemplate(
      digestLayout, address, message.recipient, JArray(data), partIds, cache
    ))
  }

  private def renderBaseTemplate(
    templates: ChannelTemplateMap,
    address  : ChannelAddress,
    message  : Message,
    data     : JArray,
    cache    : TemplateCache
  ): Option[JValue] = {
    val template = templates.get(address.channel)
    val partIDs = JArray(message.partIds.map(id => JString(id.toString)).toList)
    val tags = JArray(message.tags.map(JString).toList)

    val value = JObject(List(
      ("recipient",    message.recipient.data),
      ("data",         data),
      ("address",      JString(address.address)),
      ("senderIds",    JArray(message.senderIds.map(JString).toList)),
      ("messageId",    JString(message.id.toString)),
      ("recipientId",  JString(message.recipient.id)),
      ("channel",      JString(address.channel.name)),
      ("sentInterval", JString(message.sentInterval.toString)),
      ("tags",         tags),
      ("partIds",      partIDs),
      ("deliveryId",   JString(address.deliveryId.get.toString))
    ))

    template.flatMap(t => Try(renderTemplate(t, value, cache)).toOption)
  }

  private def renderLayoutTemplate(
    templates: ChannelTemplateMap,
    address  : ChannelAddress,
    recipient: Recipient,
    data     : JArray,
    partIds  : List[String],
    cache    : TemplateCache

  ): Option[JValue] = {
    val template = templates.get(address.channel)

    val value = JObject(List(
      ("recipient",   recipient.data),
      ("data",        data),
      ("address",     JString(address.address)),
      ("recipientId", JString(recipient.id)),
      ("channel",     JString(address.channel.name)),
      ("partIds",     JArray(partIds.map(JString))),
      ("deliveryId",  JString(address.deliveryId.get.toString))
    ))

    template.flatMap(t => Try(renderTemplate(t, value, cache)).toOption)
  }

  private def renderTemplate(
    template: JValue,
    data    : JObject,
    cache   : TemplateCache
  ): JValue = {
    template.transform {
      case JObject(List((templateName: String, JString(s)))) =>
        JString(renderTemplateString(templateName, s, data, cache))
    }
  }

  private def renderTemplateString(
    templateName: String,
    template    : String,
    data        : JObject,
    cache       : TemplateCache
  ): String = {
    templateName match {
      case "template/handlebars" => cache.renderHandlebars(template, data)
      case "template/pebble" => cache.renderPebble(template, data)
      case _ => template
    }
  }
}
