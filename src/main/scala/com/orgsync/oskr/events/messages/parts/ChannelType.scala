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

import org.json4s._

sealed trait ChannelType {
  def name: String
  def defaultDelay: Long
}

case object Web extends ChannelType {
  val name = "web"
  val defaultDelay = 0L
}

case object Push extends ChannelType {
  val name = "push"
  val defaultDelay = 5000L
}

case object SMS extends ChannelType {
  val name = "sms"
  val defaultDelay = 30000L
}

case object Email extends ChannelType {
  val name = "email"
  val defaultDelay = 60000L
}

object ChannelTypeSerializer extends CustomSerializer[ChannelType](f => ( {
  case JString(Web.name) => Web
  case JString(SMS.name) => SMS
  case JString(Push.name) => Push
  case JString(Email.name) => Email
}, {
  case channelType: ChannelType => JString(channelType.name)
}))