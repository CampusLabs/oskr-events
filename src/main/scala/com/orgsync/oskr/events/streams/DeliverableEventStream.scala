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

import com.orgsync.oskr.events.messages._
import com.orgsync.oskr.events.messages.events.Acknowledgement
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, SplitStream}
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger

object DeliverableEventStream {
  val SendEvents = "send"
  val ReadEvents = "read"

  private def getSendEvents(
    deliverables: DataStream[Either[Message, Digest]]
  ): DataStream[Either[Send, Read]] = deliverables
    .map(d =>
      Left(Send(d.merge.id, d.merge.recipient.id)): Either[Send, Read]
    ).name("send_event")

  private def getReadEvents(
    deliverables : DataStream[Either[Message, Digest]],
    events       : DataStream[Event],
    configuration: Configuration
  ): DataStream[Either[Send, Read]] = {
    val maxDeliveryTime = Time.milliseconds(
      Duration
        .parse(configuration.getString("maxDeliveryTime", "PT168H"))
        .toMillis
    )

    val ackIds = events
      .filter(_.action == Acknowledgement).name("filter_acks")
      .map(_.deliveryId).name("delivery_id")

    deliverables
      .flatMap(d => d.merge.channels.flatMap(c =>
        c.deliveryId.map(id =>
          (d.merge.id, d.merge.recipient.id, id)
        )
      )).name("delivery_id")
      .join(ackIds)
      .where(_._3).equalTo(id => id)
      .window(GlobalWindows.create())
      .trigger(CountTrigger.of(1))
      .evictor(TimeEvictor.of(maxDeliveryTime))
      .apply((t, aid) => Right(Read(t._1, t._2)): Either[Send, Read]).name("read_event")
      .uid("read_events")
  }

  def getStream(
    deliverables : DataStream[Either[Message, Digest]],
    events       : DataStream[Event],
    configuration: Configuration
  ): SplitStream[Either[Send, Read]] = {
    val sendEvents = getSendEvents(deliverables)
    val readEvents = getReadEvents(deliverables, events, configuration)

    sendEvents
      .union(readEvents)
      .split(e => e match {
        case Left(_) => List(SendEvents)
        case Right(_) => List(ReadEvents)
      })
  }
}
