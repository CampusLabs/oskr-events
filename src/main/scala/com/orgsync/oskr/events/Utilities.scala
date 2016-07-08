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

package com.orgsync.oskr.events

import java.util.Properties

import com.orgsync.oskr.events.messages.specifications.ChannelType
import org.apache.flink.configuration.Configuration

object Utilities {
  def kafkaProperties(parameters: Configuration): Properties = {
    val properties = new Properties

    val bootstrapServers = parameters.getString("kafkaBootstrap", "kafka:9092")
    val groupId = parameters.getString("kafkaGroup", "oskr-events")

    properties.setProperty("bootstrap.servers", bootstrapServers)
    properties.setProperty("group.id", groupId)
    properties.setProperty("auto.offset.reset", "earliest")

    properties
  }

  def watermarks(parameters: Configuration) = {
    parameters.getString("watermarks", "periodic") match {
      case "periodic" => 'periodic
      case "bounded" => 'bounded
      case _ => 'periodic
    }
  }

  def channelDelay(parameters: Configuration, c: ChannelType): Long = {
    val paramName = s"${c.name.toUpperCase}ChannelDelay"
    parameters.getLong(paramName, c.defaultDelay)
  }
}
