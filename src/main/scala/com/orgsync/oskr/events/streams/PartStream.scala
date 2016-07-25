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
import com.orgsync.oskr.events.messages.{BoundedPartWatermarkAssigner, PeriodicPartWatermarkAssigner, Part, PartParser}
import com.softwaremill.quicklens._
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{SplitStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.util.Collector

class ParsePart(parameters: Configuration)
  extends RichFlatMapFunction[String, Part] {

  var parser: PartParser = _

  override def flatMap(json: String, out: Collector[Part]): Unit = {
    parser.parsePart(json).foreach(out.collect)
  }

  override def open(parameters: Configuration): Unit = {
    parser = new PartParser(parameters)
  }
}

object PartStream {
  val Grouped = "grouped"
  val Ungrouped = "ungrouped"

  def getStream(
    env: StreamExecutionEnvironment,
    configuration: Configuration
  ): SplitStream[Part] = {
    val partSource = new FlinkKafkaConsumer09[String](
      configuration.getString("kafkaPartTopic", "MessageParts"),
      new SimpleStringSchema,
      Utilities.kafkaProperties(configuration)
    )

    val watermarkFormat = Utilities.watermarks(configuration)
    val watermarkAssigner = if (watermarkFormat == 'periodic)
      new PeriodicPartWatermarkAssigner(
        configuration.getLong("maxPartLag", 5000)
      )
    else
      new BoundedPartWatermarkAssigner(
        Time.milliseconds(configuration.getLong("maxPartOutOfOrder", 5000))
      )

    val dedupeCacheTime = configuration.getLong("dedupeCacheTime", 60)
    val keyFunction = (s: Part) => (s.id, s.recipient.id)

    env
      .addSource(partSource)
      .uid("part source")
      .flatMap(new ParsePart(configuration))
      .assignTimestampsAndWatermarks(watermarkAssigner)
      .keyBy(keyFunction)
      .filter(new DedupeFilterFunction[Part, (String, String)](
        keyFunction, dedupeCacheTime
      ))
      .uid("deduplicate parts")
      .map(s => s.modify(_.channels).using(_.sortBy(_.delay)))
      .split(s =>
        s.groupingKey match {
          case Some(_) => List(Grouped)
          case None => List(Ungrouped)
        }
      )
  }
}
