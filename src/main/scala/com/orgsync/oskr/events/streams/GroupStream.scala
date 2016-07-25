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

import com.orgsync.oskr.events.messages.{Message, Part, Parts}
import com.orgsync.oskr.events.streams.grouping.PartGroupingWindows
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class GroupStream(parameters: ParameterTool) {
  private val groupingGap = Time.minutes(parameters.getLong("groupingGap", 5))
    .toMilliseconds

  private val allowedLateness = Time.minutes(parameters.getLong("allowedLateness", 60))

  private val reducePartsWindow = (
    key: (String, Option[String]),
    w: TimeWindow,
    parts: Iterable[Part],
    out: Collector[Message]
  ) => Parts.toMessage(parts).foreach(out.collect)

  def getStream(partStream: DataStream[Part]): DataStream[Message] = {
    partStream
      .keyBy(p => (p.recipient.id, p.groupingKey))
      .window(new PartGroupingWindows(groupingGap))
      .allowedLateness(allowedLateness)
      .apply(reducePartsWindow)
      .uid("grouped messages")
  }
}
