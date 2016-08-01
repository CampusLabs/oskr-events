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

package com.orgsync.oskr.events.streams.delivery

import com.orgsync.oskr.events.messages.{Delivery, InstantSerializer}
import com.orgsync.oskr.events.messages.parts.{ChannelAddressSerializer, ChannelTypeKeySerializer, ChannelTypeSerializer}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.json4s.DefaultFormats
import org.json4s._
import org.json4s.jackson.Serialization.write

class SerializeDelivery extends RichMapFunction[Delivery, String] {
  implicit var formats: Formats = _

  override def map(in: Delivery): String = write(in)

  override def open(parameters: Configuration): Unit = {
    formats = DefaultFormats + InstantSerializer +
      ChannelTypeSerializer + ChannelTypeKeySerializer +
      new ChannelAddressSerializer(parameters)
  }
}
