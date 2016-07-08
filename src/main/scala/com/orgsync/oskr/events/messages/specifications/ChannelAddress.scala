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

package com.orgsync.oskr.events.messages.specifications

import com.orgsync.oskr.events.Utilities
import org.apache.flink.configuration.Configuration
import org.json4s._

import scala.math.abs

sealed trait ChannelAddress {
  def channel: ChannelType

  def address: String

  def delay: Long
}

final case class WebChannelAddress(
  address: String,
  delay  : Long
) extends ChannelAddress {
  val channel = Web
}

final case class PushChannelAddress(
  address: String,
  delay  : Long
) extends ChannelAddress {
  val channel = Push
}

final case class SMSChannelAddress(
  address: String,
  delay  : Long
) extends ChannelAddress {
  val channel = SMS
}

final case class EmailChannelAddress(
  address: String,
  delay  : Long
) extends ChannelAddress {
  val channel = Email
}

class ChannelAddressSerializer(parameters: Configuration)
  extends CustomSerializer[ChannelAddress](f => ( {
    case JObject(JField("type", JString(t)) :: JField("address", JString(a)) :: rs) =>
      val channelType = t match {
        case Web.name => Web
        case Push.name => Push
        case SMS.name => SMS
        case Email.name => Email
      }

      val delay = rs match {
        case List(JField("delay", JInt(d))) => abs(d.toLong)
        case _ => Utilities.channelDelay(parameters, channelType)
      }

      channelType match {
        case Web => WebChannelAddress(a, delay)
        case Push => PushChannelAddress(a, delay)
        case SMS => SMSChannelAddress(a, delay)
        case Email => EmailChannelAddress(a, delay)
      }
  }, {
    case channelAddress: ChannelAddress =>
      new JObject(JField("type", JString(channelAddress.channel.name)) ::
        JField("address", JString(channelAddress.address)) ::
        JField("delay", JInt(channelAddress.delay)) :: Nil)
  }))
