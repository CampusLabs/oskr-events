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

package com.orgsync.oskr.events

import com.orgsync.oskr.events.messages.{Message, Part, Parts}
import com.orgsync.oskr.events.streams.grouping.PartGroupingWindows
import com.orgsync.oskr.events.streams.{DeliveryStream, EventStream, PartStream}
import org.apache.flink.api.scala._
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Events {
  def main(args: Array[String]): Unit = {
    val parameters = ParameterTool.fromArgs(args)
    val configuration = parameters.getConfiguration
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.enableCheckpointing(parameters.getLong("checkpointInterval", 5000))
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val statePath = parameters.get("stateBackendPath", "file:///tmp/state.db")
    val backend = new RocksDBStateBackend(statePath)
    env.setStateBackend(backend)

    val partStream = PartStream.getStream(env, configuration)
    val eventStream = EventStream.getStream(env, configuration)

    val groupingGap = Time.minutes(parameters.getLong("groupingGap", 5))
      .toMilliseconds

    val allowedLateness = Time.minutes(parameters.getLong("allowedLateness", 60))

    val reducePartsWindow = (
      key: (String, Option[String]),
      w: TimeWindow,
      parts: Iterable[Part],
      out: Collector[Message]
    ) => Parts.toMessage(parts).foreach(out.collect)

    val groupedStream = partStream
      .select(PartStream.Grouped)
      .keyBy(p => (p.recipient.id, p.groupingKey))
      .window(new PartGroupingWindows(groupingGap))
      .allowedLateness(allowedLateness)
      .apply(reducePartsWindow)

    val ungroupedStream = partStream
      .select(PartStream.Ungrouped)
      .map(_.toMessage)

    val messageStream = ungroupedStream.union(groupedStream)

    val sendStream = DeliveryStream.getstream(messageStream, eventStream)
    sendStream.map(m => (m.address, m.sourceIds.toList)).print

    env.execute("oskr event processing")
  }
}
