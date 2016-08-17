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

import com.orgsync.oskr.events.Utilities
import com.orgsync.oskr.events.messages.{Digest, Message}
import com.orgsync.oskr.events.messages.parts.ChannelType
import com.orgsync.oskr.events.streams.digests.ScheduleDigestTrigger
import com.softwaremill.quicklens._
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, SplitStream}
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector
import org.threeten.extra.Interval

import scala.collection.mutable

class DigestedStream(parameters: Configuration) {
  private def getDeliverablesWithIds(
    deliverables: DataStream[Either[Message, Digest]]
  ): DataStream[Either[Message, Digest]] = {
    deliverables.map(deliverable => {
      val channels = deliverable.merge.channels.map(c => {
        val source = deliverable.merge.id + c.channel.name
        val id = UUID.nameUUIDFromBytes(source.getBytes)
        c.modify(_.deliveryId).setTo(Option(id))
      })

      deliverable match {
        case Left(m) => Left(m.modify(_.channels).setTo(channels))
        case Right(d) => Right(d.modify(_.channels).setTo(channels))
      }
    }: Either[Message, Digest]).name("add_delivery_ids")
  }

  private def extractDigests(messageStream: DataStream[Message]): SplitStream[Message] = {
    messageStream.flatMap(message => {
      val digestChannels = message.digest
        .map(_.channels).getOrElse(List[ChannelType]()).toSet

      val (digestedAddr, immediateAddr) = message.channels.partition(address =>
        digestChannels(address.channel)
      )

      val messages = mutable.MutableList[Message]()
      if (digestedAddr.nonEmpty)
        messages += message.modify(_.channels).setTo(digestedAddr)

      if (immediateAddr.nonEmpty)
        messages += message
          .modify(_.channels).setTo(immediateAddr)
          .modify(_.digest).setTo(None)

      messages
    }).name("split_digest")
      .split(d => d.digest match {
      case Some(_) => List("digests")
      case None => List("immediate")
    })
  }

  private val reduceDigest = (
    key: (String, String),
    window         : GlobalWindow,
    messageIterable: Iterable[Message],
    out            : Collector[Digest]
  ) => {
    val messages = messageIterable.toList.sortBy(_.sentInterval.getEnd)
    val emittedAt = messages
      .lastOption
      .flatMap(_.digest)
      .map(_.at)
      .getOrElse(Instant.now)

    val idBuf = new StringBuilder
    val senderIds = mutable.Set[String]()
    var lastMessage = Option.empty[Message]
    var sentInterval = Option.empty[Interval]
    val tags = mutable.Set[String]()
    val partIds = mutable.Set[String]()

    messages.foreach(message => {
      idBuf ++= message.digestKey
      idBuf ++= message.id.toString
      senderIds ++= message.senderIds
      sentInterval = Option(Utilities.mergeIntervals(
        sentInterval.getOrElse(message.sentInterval), message.sentInterval
      ))
      tags ++= message.tags
      partIds ++= message.partIds
      lastMessage = Option(message)
    })

    val id = UUID.nameUUIDFromBytes(idBuf.toString.getBytes)
    val interval = sentInterval.getOrElse(Interval.of(Instant.EPOCH, Duration.ZERO))

    lastMessage.foreach(message => out.collect(Digest(
      id, emittedAt, senderIds.toSet, message.recipient, message.channels,
      interval, tags.toSet, message.templates, partIds.toSet, messages
    )))
  }

  private val allowedLateness = Time.milliseconds(
    Duration.parse(parameters.getString("allowedLateness", "PT1H")).toMillis
  )

  private def digestStream(messageStream: DataStream[Message]): DataStream[Digest] = {
    messageStream
      .keyBy(m => (m.recipient.id, m.digestKey))
      .window(GlobalWindows.create())
      .allowedLateness(allowedLateness)
      .trigger(new ScheduleDigestTrigger)
      .apply(reduceDigest).name("digest_window")
      .uid("digested_messages")
  }

  def getStream(messageStream: DataStream[Message]): DataStream[Either[Message, Digest]] = {
    val splitStream = extractDigests(messageStream)

    val immediates: DataStream[Either[Message, Digest]] = splitStream
      .select("immediate").map(Left(_))
    immediates.name("wrap_message")

    val digests: DataStream[Either[Message, Digest]] = digestStream(splitStream.select("digests"))
      .map(Right(_))
    digests.name("wrap_digest")

    getDeliverablesWithIds(immediates.union(digests))
  }
}
