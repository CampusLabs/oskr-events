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

import com.orgsync.oskr.events.Utilities
import com.orgsync.oskr.events.messages.Event
import com.orgsync.oskr.events.parsers.EventParser
import com.orgsync.oskr.events.watermarks.{BoundedEventWatermarkAssigner, PeriodicEventWatermarkAssigner}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

object EventStream {
  private val eventParser = EventParser.parseEvent _

  def getStream(
    env          : StreamExecutionEnvironment,
    configuration: Configuration
  ): DataStream[Event] = {
    val eventSource = new FlinkKafkaConsumer09[String](
      configuration.getString("kafkaEventTopic", "Communications.Events"),
      new SimpleStringSchema,
      Utilities.kafkaConsumerProperties(configuration)
    )

    val watermarkFormat = Utilities.watermarks(configuration)
    val watermarkAssigner = if (watermarkFormat == 'periodic)
      new PeriodicEventWatermarkAssigner(
        Duration.parse(configuration.getString("maxEventLag", "PT5S")).toMillis
      )
    else
      new BoundedEventWatermarkAssigner(
        Duration.parse(configuration.getString("maxEventOutOfOrder", "PT5S")).toMillis
      )

    env
      .addSource(eventSource)
      .uid("event source")
      .flatMap(eventParser(_))
      .assignTimestampsAndWatermarks(watermarkAssigner)
  }
}
