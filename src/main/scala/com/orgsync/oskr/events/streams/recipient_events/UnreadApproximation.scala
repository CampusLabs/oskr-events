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

package com.orgsync.oskr.events.streams.recipient_events

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus
import com.orgsync.oskr.events.messages.{DeliverableEvent, Read, Send}

import scala.math._

final case class UnreadApproximation(
  sent: HyperLogLogPlus,
  read: HyperLogLogPlus
) {
  def add(deliverableEvent: DeliverableEvent): Boolean = {
    deliverableEvent match {
      case Send(id, _, _) => sent.offer(id)
      case Read(id, _, _) => read.offer(id)
    }
  }

  def sentApproximation: Long = {
    sent.cardinality()
  }

  def unreadApproximation: Long = {
    max(sent.cardinality() - read.cardinality(), 0)
  }
}

object UnreadApproximation {
  def apply() = {
    val precision = 14
    val sparsePrecision = 20

    new UnreadApproximation(
      new HyperLogLogPlus(precision, sparsePrecision),
      new HyperLogLogPlus(precision, sparsePrecision)
    )
  }
}
