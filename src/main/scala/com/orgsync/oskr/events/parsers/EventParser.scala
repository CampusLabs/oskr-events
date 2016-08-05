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

import com.orgsync.oskr.events.messages.Event
import com.orgsync.oskr.events.serializers.{ChannelTypeSerializer, EventTypeSerializer, InstantSerializer}
import org.json4s.DefaultFormats
import org.json4s.ext.UUIDSerializer
import org.json4s.jackson.JsonMethods._

import scala.util.{Failure, Success, Try}

object EventParser {
  implicit val formats = DefaultFormats + InstantSerializer +
    ChannelTypeSerializer + EventTypeSerializer + UUIDSerializer

  def parseEvent(json: String): Option[Event] = {
    val parsed = Try {
      parse(json).extract[Event]
    }

    parsed match {
      case Success(s) => Option(s)
      case Failure(e) =>
        println(e.getMessage)
        None
    }
  }
}
