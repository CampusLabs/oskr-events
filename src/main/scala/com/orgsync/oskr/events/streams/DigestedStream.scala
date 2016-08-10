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

import com.orgsync.oskr.events.messages.{Digest, Message}
import com.orgsync.oskr.events.messages.parts.ChannelType
import com.orgsync.oskr.events.streams.digests.ScheduleDigestTrigger
import com.softwaremill.quicklens._
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, SplitStream}
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector

import scala.collection.mutable

object DigestedStream {
  private val Digests = "digests"
  private val Immediate = "immediate"

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
    }).split(m => m.digest match {
      case Some(_) => List(Digests)
      case None => List(Immediate)
    })
  }

  private val createDigest = (
    key     : (String, String),
    window  : GlobalWindow,
    messages: Iterable[Message],
    out     : Collector[Digest]
  ) => {
    
  }

  private def digestStream(messageStream: DataStream[Message]): DataStream[Digest] = {
    messageStream
      .keyBy(m => (m.recipient.id, m.digest.map(_.key).getOrElse("default")))
      .window(GlobalWindows.create())
      .trigger(new ScheduleDigestTrigger)
      .apply(createDigest)
  }

  def getStream(messageStream: DataStream[Message]): DataStream[Either[Message, Digest]] = {
    val splitStream = extractDigests(messageStream)

    val immediates: DataStream[Either[Message, Digest]] = splitStream
      .select(Immediate).map(m => Left(m))

    val digests: DataStream[Either[Message, Digest]] = digestStream(splitStream.select(Digests))
      .map(d => Right(d))

    immediates.union(digests)
  }
}
