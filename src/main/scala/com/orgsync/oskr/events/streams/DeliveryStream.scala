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

import com.orgsync.oskr.events.messages._
import com.orgsync.oskr.events.streams.deliveries.{AssignChannel, ScheduleChannelTrigger}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, SplitStream}
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows

object DeliveryStream {
  def getStream(
    deliverables     : DataStream[Either[Message, Digest]],
    deliverableEvents: SplitStream[Either[Send, Read]],
    configuration    : Configuration
  ): DataStream[Delivery] = {
    val readEventIds = deliverableEvents
      .select(DeliverableEventStream.ReadEvents)
      .map(_.merge.id).name("deliverable_id")

    deliverables
      .coGroup(readEventIds)
      .where(_.merge.id).equalTo(id => id)
      .window(GlobalWindows.create)
      .trigger(new ScheduleChannelTrigger)
      .apply(new AssignChannel).name("delivery_window")
      .uid("deliveries")
  }
}
