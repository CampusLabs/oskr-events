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

package com.orgsync.oskr.events.streams.grouping

import java.util

import scala.collection.JavaConversions._
import com.orgsync.oskr.events.messages.Part
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner
import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner.MergeCallback
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner.WindowAssignerContext
import org.apache.flink.streaming.api.windowing.triggers.{EventTimeTrigger, Trigger}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

import scala.math.abs

class PartGroupingWindows(defaultGap: Long)
  extends MergingWindowAssigner[Part, TimeWindow] {

  override def isEventTime: Boolean = true

  override def assignWindows(
    part     : Part,
    timestamp: Long,
    windowAssignerContext: WindowAssignerContext
  ): util.Collection[TimeWindow] = {
    val gap = abs(part.groupingGap.getOrElse(defaultGap))
    List(new TimeWindow(timestamp, timestamp + gap))
  }

  override def mergeWindows(
    windows: util.Collection[TimeWindow],
    mergeCallback: MergeCallback[TimeWindow]
  ): Unit = TimeWindow.mergeWindows(windows, mergeCallback)

  override def getDefaultTrigger(
    streamExecutionEnvironment: StreamExecutionEnvironment
  ): Trigger[Part, TimeWindow] = {
    EventTimeTrigger.create().asInstanceOf[Trigger[Part, TimeWindow]]
  }

  override def getWindowSerializer(
    executionConfig: ExecutionConfig
  ): TypeSerializer[TimeWindow] = {
    new TimeWindow.Serializer
  }
}
