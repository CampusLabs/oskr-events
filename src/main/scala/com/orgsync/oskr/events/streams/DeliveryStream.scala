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

import java.lang.Iterable

import com.orgsync.oskr.events.messages.events.Acknowledgement
import com.orgsync.oskr.events.messages.specifications.ChannelAddress
import com.orgsync.oskr.events.messages.{Event, Specification}
import com.orgsync.oskr.events.streams.delivery.ScheduleChannelTrigger
import org.apache.flink.api.common.functions.RichCoGroupFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

object DeliveryStream {
  private class AssignChannel
    extends RichCoGroupFunction[Specification, Event, (Specification, ChannelAddress)] {

    var index: ValueState[Int] = _

    override def coGroup(
      specifications: Iterable[Specification],
      events        : Iterable[Event],
      out           : Collector[(Specification, ChannelAddress)]
    ): Unit = specifications.asScala.take(1).foreach { specification =>
      val currentIndex = index.value
      val channels = specification.channels
      val channelCount = channels.length
      val acked = events.asScala.exists(_.action == Acknowledgement)

      if (acked)
        index.clear()
      else if (currentIndex < channelCount) {
        out.collect((specification, channels(currentIndex)))
        index.update(currentIndex + 1)

        if (currentIndex == channelCount - 1)
          index.clear()
      }
    }

    override def open(parameters: Configuration): Unit = {
      val descriptor = new ValueStateDescriptor[Int](
        "channelIndex", classOf[Int], 0
      )

      index = getRuntimeContext.getState(descriptor)
    }
  }

  def getstream(
    specifications: DataStream[Specification],
    events: DataStream[Event]
  ): DataStream[(Specification, ChannelAddress)] = {
    val ackEvents = events.filter(_.action == Acknowledgement)

    specifications
      .coGroup(ackEvents)
      .where(_.id)
      .equalTo(_.messageId)
      .window(GlobalWindows.create)
      .trigger(new ScheduleChannelTrigger)
      .apply(new AssignChannel)
  }
}
