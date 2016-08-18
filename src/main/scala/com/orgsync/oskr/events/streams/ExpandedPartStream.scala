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
import java.util.UUID

import com.orgsync.oskr.events.Utilities
import com.orgsync.oskr.events.filters.DedupeFilterFunction
import com.orgsync.oskr.events.messages.parts.ChannelAddress
import com.orgsync.oskr.events.messages.ExpandedPart
import com.orgsync.oskr.events.streams.parts.ParsePart
import com.orgsync.oskr.events.watermarks.{BoundedPartWatermarkAssigner, PeriodicPartWatermarkAssigner}
import com.softwaremill.quicklens._
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{SplitStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

object ExpandedPartStream {
  val Grouped = "grouped"
  val Ungrouped = "ungrouped"

  def getStream(
    env          : StreamExecutionEnvironment,
    configuration: Configuration
  ): SplitStream[ExpandedPart] = {
    val partSource = new FlinkKafkaConsumer09[String](
      configuration.getString("kafkaPartTopic", "Communications.MessageParts"),
      new SimpleStringSchema,
      Utilities.kafkaConsumerProperties(configuration)
    )

    val watermarkFormat = Utilities.watermarks(configuration)
    val watermarkAssigner = if (watermarkFormat == 'periodic)
      new PeriodicPartWatermarkAssigner(
        Duration.parse(configuration.getString("maxPartLag", "PT5S")).toMillis
      )
    else
      new BoundedPartWatermarkAssigner(
        Duration.parse(configuration.getString("maxPartOutOfOrder", "PT5S")).toMillis
      )

    val dedupeCacheTime = Duration.parse(
      configuration.getString("dedupeCacheTime", "PT1H")
    )
    val keyFunction = (s: ExpandedPart) => (s.id, s.recipient.id)

    env
      .addSource(partSource).name("part_source")
      .uid("part_source")
      .flatMap(new ParsePart(configuration)).name("parse_part")
      .assignTimestampsAndWatermarks(watermarkAssigner).name("assign_part_timestamp")
      .flatMap(_.toExpandedParts).name("expand_parts")
      .keyBy(keyFunction)
      .filter(new DedupeFilterFunction[ExpandedPart, (String, String)](
        keyFunction, dedupeCacheTime
      )).name("deduplicate_parts")
      .uid("deduplicate_parts")
      .map(_.modify(_.recipient.channels).using(_.sortBy(_.delay))).name("sort_channels")
      .split(s =>
        s.groupingKey match {
          case Some(_) => List(Grouped)
          case None => List(Ungrouped)
        }
      )
  }
}
