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

import com.orgsync.oskr.events.messages._
import com.orgsync.oskr.events.messages.delivery_events.Acknowledgement
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, SplitStream}

object DeliverableEventStream {
  val SendEvents = "send"
  val ReadEvents = "read"

  private def getSendEvents(
    deliverables: DataStream[Either[Message, Digest]]
  ): DataStream[Either[Send, Read]] = deliverables
    .map(d =>
      Left(Send(
        d.merge.id, d.merge.recipient.id, d.merge.emittedAt
      )): Either[Send, Read]
    ).name("send_event")

  private def getReadEvents(
    events: DataStream[DeliveryEvent]
  ): DataStream[Either[Send, Read]] = {
    events
      .filter(_.action == Acknowledgement).name("filter_acks")
      .map(e => Right(Read(
        e.deliverableId, e.recipientId, e.at
      )): Either[Send, Read])
      .name("read_event")
  }

  def getStream(
    deliverables: DataStream[Either[Message, Digest]],
    events: DataStream[DeliveryEvent]
  ): SplitStream[Either[Send, Read]] = {
    val sendEvents = getSendEvents(deliverables)
    val readEvents = getReadEvents(events)

    sendEvents
      .union(readEvents)
      .split(e => e match {
        case Left(_) => List(SendEvents)
        case Right(_) => List(ReadEvents)
      })
  }
}
