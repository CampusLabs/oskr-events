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

import com.orgsync.oskr.events.messages.specifications._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.json4s._
import org.json4s.native.JsonMethods._

import scala.util.{Failure, Success, Try}

final case class Specification(
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

class SpecificationParser(parameters: Configuration) {
  val channelAddressSerializer = new ChannelAddressSerializer(parameters)

  implicit val formats = DefaultFormats + InstantSerializer +
    ChannelTypeSerializer + channelAddressSerializer + TemplateFormatSerializer

  def parseSpecification(json: String): Option[Specification] = {
    val parsed = Try {
      parse(json).extract[Specification]
    }

    parsed match {
      case Success(s) => Option(s)
      case Failure(e) =>
        println(e.getMessage)
        None
    }
  }
}

class BoundedSpecificationWatermarkAssigner(time: Time)
  extends BoundedOutOfOrdernessTimestampExtractor[Specification](time) {
  override def extractTimestamp(s: Specification) = s.sentAt.toEpochMilli
}

class PeriodicSpecificationWatermarkAssigner(maxTimeLag: Long)
  extends AssignerWithPeriodicWatermarks[Specification] {

  override def extractTimestamp(s: Specification, previousTimestamp: Long) = {
    s.sentAt.toEpochMilli
  }

  override def getCurrentWatermark: Watermark = {
    new Watermark(System.currentTimeMillis() - maxTimeLag)
  }
}
