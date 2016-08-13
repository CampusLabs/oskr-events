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

import java.time.Duration

import com.orgsync.oskr.events.messages.parts._
import com.orgsync.oskr.events.streams.deliveries.SerializeDelivery
import com.orgsync.oskr.events.streams._
import org.apache.flink.api.scala._
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Events {
  def main(args: Array[String]): Unit = {
    val parameters = ParameterTool.fromArgs(args)
    val configuration = parameters.getConfiguration
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val checkpointInterval = Duration.parse(
      parameters.get("checkpointInterval", "PT5S")
    )

    env.enableCheckpointing(checkpointInterval.toMillis)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val statePath = parameters.get("stateBackendPath", "file:///tmp/oskr")
    val backend = new RocksDBStateBackend(statePath)
    env.setStateBackend(backend)

    val partStream = PartStream.getStream(env, configuration)
    val eventStream = EventStream.getStream(env, configuration)

    val groupedStream = new GroupStream(configuration)
      .getStream(partStream.select(PartStream.Grouped))

    val ungroupedStream = partStream
      .select(PartStream.Ungrouped)
      .map(_.toMessage).name("to_message")

    val messageStream = ungroupedStream.union(groupedStream)
    val digestedStream = new DigestedStream(configuration).getStream(messageStream)

    val deliveryStream = DeliveryStream.getStream(digestedStream, eventStream, configuration)
      .split(d => List(d.channel.name))

    List(Storage, Web, SMS, Push, Email).foreach(channel => {
      val topicName = Utilities.channelTopic(configuration, channel)

      deliveryStream
        .select(channel.name)
        .map(new SerializeDelivery).name("serialize_delivery")
        .addSink(new KafkaSink(configuration).sink(topicName))
        .name(s"${channel.name}_sink")
    })

    val jobName = configuration.getString("jobName", "oskr_event_processing")
    env.execute(jobName)
  }
}
