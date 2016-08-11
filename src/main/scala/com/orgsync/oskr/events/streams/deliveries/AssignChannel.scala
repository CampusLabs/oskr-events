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

package com.orgsync.oskr.events.streams.deliveries

import java.lang.Iterable
import java.util.UUID

import com.orgsync.oskr.events.messages.{Deliverable, Delivery}
import org.apache.flink.api.common.functions.RichCoGroupFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._
class AssignChannel
  extends RichCoGroupFunction[Deliverable, UUID, Delivery] {

  var index: ValueState[Int] = _
  var cache: TemplateCache = _

  override def coGroup(
    deliverables  : Iterable[Deliverable],
    deliverableIds: Iterable[UUID],
    out           : Collector[Delivery]
  ): Unit = deliverables.asScala.take(1).foreach {
    deliverable =>
      val currentIndex = index.value
      val channels = deliverable.channels
      val channelCount = channels.length
      val acked = deliverableIds.asScala.exists(id => true)

      if (acked)
        index.clear()
      else if (currentIndex < channelCount) {
        val delay = channels(currentIndex).delay
        val triggers = channels.count(_.delay == delay)

        0 until triggers foreach {
          i =>
            val channelAddress = channels(currentIndex + i)
            deliverable.delivery(channelAddress, cache).foreach(out.collect)
        }

        index.update(currentIndex + triggers)
        if (currentIndex == channelCount - 1) index.clear()
      }
  }

  override def open(parameters: Configuration): Unit = {
    val descriptor = new ValueStateDescriptor[Int](
      "channelIndex", classOf[Int], 0
    )

    index = getRuntimeContext.getState(descriptor)
    cache = new TemplateCache
  }
}
