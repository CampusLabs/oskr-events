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

import com.orgsync.oskr.events.messages.Message
import com.orgsync.oskr.events.messages.parts.ChannelType
import com.softwaremill.quicklens._
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, SplitStream}
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows

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

  private def digestStream(messageStream: DataStream[Message]): DataStream[Message] = {
    messageStream
      .keyBy(_.recipient.id)
      .window(GlobalWindows.create())

    messageStream
  }

  def getStream(messageStream: DataStream[Message]): DataStream[Message] = {
    val splitStream = extractDigests(messageStream)

    val immediateMessages = splitStream.select(Immediate)
    val digestMessages = splitStream.select(Digests)

    immediateMessages.union(digestStream(digestMessages))
  }
}
