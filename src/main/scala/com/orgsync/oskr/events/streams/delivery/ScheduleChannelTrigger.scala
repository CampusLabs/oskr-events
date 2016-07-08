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

import com.orgsync.oskr.events.messages.events.Acknowledgement
import com.orgsync.oskr.events.messages.{Event, Specification}
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.datastream.CoGroupedStreams.TaggedUnion
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.windows.Window

class ScheduleChannelTrigger[W <: Window]
  extends Trigger[TaggedUnion[Specification, Event], W] {

  private val countDescriptor = new ValueStateDescriptor(
    "triggerCount", classOf[Int], 0
  )

  private val initDescriptor = new ValueStateDescriptor(
    "deliveryTriggerInitialized?", classOf[Boolean], false
  )

  private val ackedDescription = new ValueStateDescriptor(
    "acked?", classOf[Boolean], false
  )

  override def onElement(
    t: TaggedUnion[Specification, Event],
    timestamp     : Long,
    window        : W,
    triggerContext: TriggerContext
  ): TriggerResult = {
    val initialized = triggerContext.getPartitionedState(initDescriptor)
    val triggerCount = triggerContext.getPartitionedState(countDescriptor)
    val acked = triggerContext.getPartitionedState(ackedDescription)
    val now = triggerContext.getCurrentProcessingTime

    val specification = Option(t.getOne)
    val event = Option(t.getTwo)

    event.foreach(e => if (e.action == Acknowledgement) acked.update(true))

    if (!initialized.value)
      specification.foreach {
        s =>
          initialized.update(true)
          triggerCount.update(s.channels.length)
          s.channels.foreach {
            c =>
              triggerContext.registerProcessingTimeTimer(now + c.delay)
          }
      }

    if (acked.value) {
      initialized.clear()
      acked.clear()
      TriggerResult.FIRE_AND_PURGE
    } else {
      TriggerResult.CONTINUE
    }
  }

  override def onProcessingTime(
    timestamp: Long,
    window: W,
    triggerContext: TriggerContext
  ): TriggerResult = {
    val triggerCount = triggerContext.getPartitionedState(countDescriptor)
    val currentCount = triggerCount.value - 1

    triggerCount.update(currentCount)

    if (currentCount <= 0) {
      triggerCount.clear()
      TriggerResult.FIRE_AND_PURGE
    } else
      TriggerResult.FIRE
  }

  override def onEventTime(
    timestamp: Long,
    window: W,
    triggerContext: TriggerContext
  ): TriggerResult = TriggerResult.CONTINUE
}
