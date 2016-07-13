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

import com.orgsync.oskr.events.streams.{DeliveryStream, EventStream, PartStream}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

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

    val groupedStream = partStream.select(PartStream.Grouped)
    val ungroupedStream = partStream.select(PartStream.Ungrouped)

    val sendStream = DeliveryStream.getstream(ungroupedStream, eventStream)
    sendStream.map(t => (t._1.id, t._2)).print

    env.execute("oskr event processing")
  }
}
