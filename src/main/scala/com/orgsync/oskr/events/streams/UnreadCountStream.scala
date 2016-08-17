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
import com.orgsync.oskr.events.streams.deliverable_events.{OldestSlidingWindowTrigger, UnreadApproximation}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, SplitStream}
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

object UnreadCountStream {
  private def initialHLLState = ("", UnreadApproximation())

  private val foldHLLFn = (
    state: (String, UnreadApproximation),
    deliverableEvent: Either[Send, Read]
  ) => {
    state._2.add(deliverableEvent.merge)
    (deliverableEvent.merge.recipientId, state._2)
  }

  def getStream(
    deliverableEvents: SplitStream[Either[Send, Read]],
    configuration    : Configuration
  ): DataStream[(String, Long)] = {
    val unreadTime = Time.milliseconds(
      Duration
        .parse(configuration.getString("unreadTime", "PT168H"))
        .toMillis
    )

    val unreadSlide = Time.milliseconds(unreadTime.toMilliseconds / 2)

    val eventTrigger = new OldestSlidingWindowTrigger[Either[Send, Read], TimeWindow](
      unreadTime, unreadSlide
    )

    deliverableEvents
      .keyBy(_.merge.recipientId)
      .window(SlidingProcessingTimeWindows.of(unreadTime, unreadSlide))
      .trigger(eventTrigger)
      .fold(initialHLLState)(foldHLLFn).name("unread_window")
      .uid("unread_approximations")
      .map(state => (state._1, state._2.unreadApproximation)).name("unread")
  }
}
