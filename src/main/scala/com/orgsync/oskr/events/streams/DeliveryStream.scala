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

import java.util.UUID

import com.orgsync.oskr.events.messages._
import com.orgsync.oskr.events.streams.deliveries.{AssignChannel, ScheduleChannelTrigger}
import com.softwaremill.quicklens._
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, SplitStream}
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows

object DeliveryStream {
  private def getDeliverablesWithIds(
    deliverables: DataStream[Either[Message, Digest]]
  ): DataStream[Either[Message, Digest]] = {
    deliverables.map(deliverable => {
      val channels = deliverable.merge.channels.map(c => {
        val source = deliverable.merge.id + c.channel.name
        val id = UUID.nameUUIDFromBytes(source.getBytes)
        c.modify(_.deliveryId).setTo(Option(id))
      })

      deliverable match {
        case Left(m) => Left(m.modify(_.channels).setTo(channels))
        case Right(d) => Right(d.modify(_.channels).setTo(channels))
      }
    }: Either[Message, Digest]).name("add_delivery_ids")
  }

  def getStream(
    deliverables     : DataStream[Either[Message, Digest]],
    deliverableEvents: SplitStream[Either[Send, Read]],
    configuration    : Configuration
  ): DataStream[Delivery] = {
    val deliverablesWithIds = getDeliverablesWithIds(deliverables)
    val readEventIds = deliverableEvents
      .select(DeliverableEventStream.ReadEvents)
      .map(_.merge.id).name("deliverable_id")

    deliverablesWithIds
      .coGroup(readEventIds)
      .where(_.merge.id).equalTo(id => id)
      .window(GlobalWindows.create)
      .trigger(new ScheduleChannelTrigger)
      .apply(new AssignChannel).name("delivery_window")
      .uid("deliveries")
  }
}
