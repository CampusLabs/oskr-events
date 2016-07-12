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

import com.orgsync.oskr.events.messages.parts._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.json4s._
import org.json4s.native.JsonMethods._

import scala.util.{Failure, Success, Try}

final case class Part(
  id         : String,
  senderId   : String,
  recipientId: String,
  channels   : Array[ChannelAddress],
  sentAt     : Instant,
  groupingKey: Option[String],
  tags       : Option[Array[String]],
  digestAt   : Option[Boolean],
  immediate  : Option[Boolean],
  broadcast  : Option[Boolean],
  templates  : Array[TemplateSet],
  data       : JObject
)

class PartParser(parameters: Configuration) {
  val channelAddressSerializer = new ChannelAddressSerializer(parameters)

  implicit val formats = DefaultFormats + InstantSerializer +
    ChannelTypeSerializer + channelAddressSerializer + TemplateFormatSerializer

  def parsePart(json: String): Option[Part] = {
    val parsed = Try {
      parse(json).extract[Part]
    }

    parsed match {
      case Success(s) => Option(s)
      case Failure(e) =>
        println(e.getMessage)
        None
    }
  }
}

class BoundedPartWatermarkAssigner(time: Time)
  extends BoundedOutOfOrdernessTimestampExtractor[Part](time) {
  override def extractTimestamp(s: Part) = s.sentAt.toEpochMilli
}

class PeriodicPartWatermarkAssigner(maxTimeLag: Long)
  extends AssignerWithPeriodicWatermarks[Part] {

  override def extractTimestamp(s: Part, previousTimestamp: Long) = {
    s.sentAt.toEpochMilli
  }

  override def getCurrentWatermark: Watermark = {
    new Watermark(System.currentTimeMillis() - maxTimeLag)
  }
}