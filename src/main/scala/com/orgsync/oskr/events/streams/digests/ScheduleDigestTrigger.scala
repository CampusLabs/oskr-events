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

package com.orgsync.oskr.events.streams.digests

import java.time.Instant

import com.orgsync.oskr.events.messages.Message
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.windows.Window

class ScheduleDigestTrigger[W <: Window] extends Trigger[Message, W] {
  private val scheduledAt = new ValueStateDescriptor[Instant](
    "scheduledAt", classOf[Instant], Instant.EPOCH
  )

  override def onElement(
    message       : Message,
    timestamp     : Long,
    window        : W,
    triggerContext: TriggerContext
  ): TriggerResult = {
    val scheduledState = triggerContext.getPartitionedState(scheduledAt)
    val scheduled = scheduledState.value()
    val at = message.digest.map(_.at).getOrElse(Instant.EPOCH)
    val now = Instant.ofEpochMilli(timestamp)

    if (at.isBefore(now)) {
      triggerContext.deleteEventTimeTimer(scheduled.toEpochMilli)
      scheduledState.clear()
      TriggerResult.FIRE_AND_PURGE
    } else if (at != scheduled) {
      if (scheduled != Instant.EPOCH)
        triggerContext.deleteEventTimeTimer(scheduled.toEpochMilli)

      triggerContext.registerEventTimeTimer(at.toEpochMilli)
      scheduledState.update(at)
      TriggerResult.CONTINUE
    } else
      TriggerResult.CONTINUE
  }

  override def onEventTime(
    timestamp     : Long,
    window        : W,
    triggerContext: TriggerContext
  ): TriggerResult = {
    val scheduledState = triggerContext.getPartitionedState(scheduledAt)
    scheduledState.clear()
    TriggerResult.FIRE_AND_PURGE
  }

  override def onProcessingTime(
    timestamp     : Long,
    window        : W,
    triggerContext: TriggerContext
  ): TriggerResult = TriggerResult.CONTINUE
}
