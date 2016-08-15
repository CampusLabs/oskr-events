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

import java.time.{Duration, Instant}
import java.util.UUID

import com.orgsync.oskr.events.messages.{Message, Part}
import com.orgsync.oskr.events.streams.grouping.PartGroupingWindows
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.json4s.JsonAST.{JArray, JValue}
import org.threeten.extra.Interval

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class GroupStream(parameters: Configuration) {
  private val groupingGap = Duration.parse(parameters.getString("groupingGap", "PT5M"))

  private val allowedLateness = Time.milliseconds(
    Duration.parse(parameters.getString("allowedLateness", "PT1H")).toMillis
  )

  private val reducePartsWindow = (
    key         : (String, String),
    window      : TimeWindow,
    partIterable: Iterable[Part],
    out         : Collector[Message]
  ) => {
    val parts = partIterable.toList.sortBy(_.sentAt)
    val emittedAt = Instant.ofEpochMilli(window.getEnd)

    val idBuf = new StringBuilder
    val senderIds = mutable.Set[String]()
    var lastPart = Option.empty[Part]
    val sentAt = mutable.TreeSet[Instant]()
    val tags = mutable.Set[String]()
    val partIds = mutable.Set[String]()
    val partData = ListBuffer[JValue]()

    parts.foreach(part => {
      idBuf ++= part.recipient.id
      idBuf ++= part.id
      senderIds += part.senderId
      sentAt += part.sentAt
      tags ++= part.tags.getOrElse(Set())
      partIds += part.id
      partData += part.data
      lastPart = Option(part)
    })

    lastPart.foreach(part => {
      val id = UUID.nameUUIDFromBytes(idBuf.toString.getBytes)
      val sentInterval = Interval.of(sentAt.firstKey, sentAt.lastKey)

      out.collect(Message(
        id, emittedAt, senderIds.toSet, part.recipient, part.channels,
        sentInterval, tags.toSet, part.digest, part.templates,
        partIds.toSet, JArray(partData.toList)
      ))
    })
  }

  def getStream(partStream: DataStream[Part]): DataStream[Message] = {
    partStream
      .keyBy(p => (p.recipient.id, p.groupingKey.getOrElse("default")))
      .window(new PartGroupingWindows(groupingGap))
      .allowedLateness(allowedLateness)
      .apply(reducePartsWindow).name("group_window")
      .uid("grouped_messages")
  }
}
