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
import java.util.UUID

import com.orgsync.oskr.events.messages.events.{EventType, EventTypeSerializer}
import com.orgsync.oskr.events.messages.parts.ChannelTypeSerializer
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.ext.UUIDSerializer

import scala.util.{Failure, Success, Try}

case class Event(
  deliveryId: UUID,
  action    : EventType,
  at        : Instant,
  data      : JValue
)

object EventParser {
  implicit val formats = DefaultFormats + InstantSerializer +
    ChannelTypeSerializer + EventTypeSerializer + UUIDSerializer

  def parseEvent(json: String): Option[Event] = {
    val parsed = Try {
      parse(json).extract[Event]
    }

    parsed match {
      case Success(s) => Option(s)
      case Failure(e) =>
        println(e.getMessage)
        None
    }
  }
}

class BoundedEventWatermarkAssigner(bound: Long)
  extends BoundedOutOfOrdernessTimestampExtractor[Event](Time.milliseconds(bound)) {
  override def extractTimestamp(s: Event) = s.at.toEpochMilli
}

class PeriodicEventWatermarkAssigner(maxTimeLag: Long)
  extends AssignerWithPeriodicWatermarks[Event] {

  override def extractTimestamp(s: Event, previousTimestamp: Long) = {
    s.at.toEpochMilli
  }

  override def getCurrentWatermark: Watermark = {
    new Watermark(System.currentTimeMillis() - maxTimeLag)
  }
}
