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
import com.orgsync.oskr.events.messages.{Digest, Message}
import com.orgsync.oskr.events.messages.parts.ChannelType
import com.orgsync.oskr.events.streams.digests.ScheduleDigestTrigger
import com.softwaremill.quicklens._
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, SplitStream}
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.mutable

class DigestedStream(parameters: Configuration) {
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
    }).split(d => d.digest match {
      case Some(_) => List("digests")
      case None => List("immediate")
    })
  }

  private val reduceDigest = (
    digestOption: Option[Digest],
    message: Message
  ) => digestOption match {
    case None => Option(message.toDigest)
    case Some(digest) =>
      val idSource = digest.id.toString + message.id.toString
      val id = UUID.nameUUIDFromBytes(idSource.getBytes)
      val senderIds = digest.senderIds ++ message.senderIds
      val sentInterval = Utilities.mergeIntervals(
        digest.sentInterval, message.sentInterval
      )
      val tags = digest.tags ++ message.tags
      val partIds = digest.partIds ++ message.partIds
      val messages = digest.messages :+ message

      Option(Digest(
        id, senderIds, message.recipient, message.channels, sentInterval,
        tags, message.templates, partIds, messages
      ))
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
      .fold(Option.empty[Digest])(reduceDigest)
      .flatMap(identity(_))
  }

  def getStream(messageStream: DataStream[Message]): DataStream[Either[Message, Digest]] = {
    val splitStream = extractDigests(messageStream)

    val immediates: DataStream[Either[Message, Digest]] = splitStream
      .select("immediate").map(Left(_))

    val digests: DataStream[Either[Message, Digest]] = digestStream(splitStream.select("digests"))
      .map(Right(_))

    immediates.union(digests)
  }
}
