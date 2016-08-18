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

package com.orgsync.oskr.events.serializers

import java.time.Duration

import com.orgsync.oskr.events.Utilities
import com.orgsync.oskr.events.messages.parts._
import org.apache.flink.configuration.Configuration
import org.json4s.CustomSerializer
import org.json4s.JsonAST.{JField, JObject, JString}

class ChannelAddressSerializer(parameters: Configuration)
  extends CustomSerializer[ChannelAddress](f => ( {
    case JObject(JField("type", JString(t)) :: JField("address", JString(a)) :: rs) =>
      val channelType = t match {
        case Storage.name => Storage
        case Web.name => Web
        case Push.name => Push
        case SMS.name => SMS
        case Email.name => Email
      }

      val delay = rs match {
        case List(JField("delay", JString(d))) => Duration.parse(d)
        case _ => Utilities.channelDelay(parameters, channelType)
      }

      channelType match {
        case Storage => StorageChannelAddress(a, delay)
        case Web => WebChannelAddress(a, delay)
        case Push => PushChannelAddress(a, delay)
        case SMS => SMSChannelAddress(a, delay)
        case Email => EmailChannelAddress(a, delay)
      }
  }, {
    case channelAddress: ChannelAddress =>
      new JObject(JField("type", JString(channelAddress.channel.name)) ::
        JField("address", JString(channelAddress.address)) ::
        JField("delay", JString(channelAddress.delay.toString)) :: Nil)
  }))
