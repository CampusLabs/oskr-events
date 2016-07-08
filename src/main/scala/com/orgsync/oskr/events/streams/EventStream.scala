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

import com.orgsync.oskr.events.Utilities
import com.orgsync.oskr.events.messages.{
  BoundedEventWatermarkAssigner,
  Event,
  EventParser,
  PeriodicEventWatermarkAssigner
}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

object EventStream {
  private val eventParser = EventParser.parseEvent _

  def getStream(
    env            : StreamExecutionEnvironment,
    configuration  : Configuration
  ): DataStream[Event] = {
    val eventSource = new FlinkKafkaConsumer09[String](
      configuration.getString("kafkaEventTopic", "Events"),
      new SimpleStringSchema,
      Utilities.kafkaProperties(configuration)
    )

    val watermarkFormat = Utilities.watermarks(configuration)
    val watermarkAssigner = if (watermarkFormat == 'periodic)
      new PeriodicEventWatermarkAssigner(
        configuration.getLong("maxEventLag", 5000)
      )
    else
      new BoundedEventWatermarkAssigner(
        Time.milliseconds(configuration.getLong("maxEventOutOfOrder", 5000))
      )

    env
      .addSource(eventSource)
      .flatMap(eventParser(_))
      .assignTimestampsAndWatermarks(watermarkAssigner)
      .keyBy(_.messageId)
  }
}
