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

package com.orgsync.oskr.events.streams

import java.time.Duration

import com.orgsync.oskr.events.messages.events.Acknowledgement
import com.orgsync.oskr.events.messages.{Delivery, Digest, Event, Message}
import com.orgsync.oskr.events.streams.deliveries.{AssignChannel, ScheduleChannelTrigger}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.assigners.{GlobalWindows, SlidingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

object DeliveryStream {
  def getStream(
    deliverables : DataStream[Either[Message, Digest]],
    events       : DataStream[Event],
    configuration: Configuration
  ): DataStream[Delivery] = {
    val ackEvents = events.filter(_.action == Acknowledgement)

    val maxDeliveryTime = Time.milliseconds(
      Duration
        .parse(configuration.getString("maxDeliveryTime", "PT168H"))
        .toMillis
    )

    val deliverySlideTime = Time.milliseconds(maxDeliveryTime.toMilliseconds / 2)

    val ackedDeliverableIds = deliverables
      .flatMap(e => {
        val deliverable = e.merge
        deliverable.channels.map(c => (deliverable.id, c.deliveryId))
      })
      .join(ackEvents)
      .where(_._2).equalTo(e => Option(e.deliveryId))
      .window(SlidingProcessingTimeWindows.of(maxDeliveryTime, deliverySlideTime))
      .trigger(CountTrigger.of[TimeWindow](1))
      .apply((t, event) => t._1)
      .uid("acked message ids")

    deliverables
      .coGroup(ackedDeliverableIds)
      .where(_.merge.id).equalTo(id => id)
      .window(GlobalWindows.create)
      .trigger(new ScheduleChannelTrigger)
      .apply(new AssignChannel)
      .uid("deliveries")
  }
}
