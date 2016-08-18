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

package com.orgsync.oskr.events.parsers

import com.orgsync.oskr.events.messages.DeliveryEvent
import com.orgsync.oskr.events.serializers.{ChannelTypeSerializer, DeliveryEventTypeSerializer, InstantSerializer}
import org.json4s.DefaultFormats
import org.json4s.ext.UUIDSerializer
import org.json4s.jackson.JsonMethods._
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

object DeliveryEventParser {
  val log = LoggerFactory.getLogger(this.getClass)

  val notNullFormats = new DefaultFormats {
    override val allowNull = false
  }

  implicit val formats = notNullFormats + InstantSerializer +
    ChannelTypeSerializer + DeliveryEventTypeSerializer + UUIDSerializer

  def parseEvent(json: String): Option[DeliveryEvent] = {
    val parsed = Try {
      parse(json).extract[DeliveryEvent]
    }

    parsed match {
      case Success(s) => Option(s)
      case Failure(e) =>
        log.warn("unable to parse delivery event", e)
        None
    }
  }
}
