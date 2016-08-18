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

package com.orgsync.oskr.events.filters

import java.io.Serializable
import java.time.Duration
import java.util.concurrent.TimeUnit
import java.{lang, util}

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.checkpoint.CheckpointedAsynchronously

import scala.collection.JavaConverters._

class DedupeFilterFunction[T, K <: Serializable](
  keySelector: T => K,
  expiration : Duration
) extends RichFilterFunction[T]
  with CheckpointedAsynchronously[util.HashSet[K]]
{
  private var dedupeCache: LoadingCache[K, lang.Boolean] = _

  override def open(parameters: Configuration): Unit = {
    createDedupeCache()
  }

  override def filter(t: T): Boolean = {
    val k = keySelector(t)
    val notSeen = !dedupeCache.get(k)

    if (notSeen) dedupeCache.put(k, true)
    notSeen
  }

  override def restoreState(t: util.HashSet[K]): Unit = {
    createDedupeCache()
    t.asScala.foreach(k => dedupeCache.put(k, true))
  }

  override def snapshotState(l: Long, l1: Long): util.HashSet[K] = {
    dedupeCache.cleanUp()
    new util.HashSet[K](dedupeCache.asMap().keySet)
  }

  private def createDedupeCache(): Unit = {
    val cacheLoader = new CacheLoader[K, lang.Boolean] {
      override def load(k: K): lang.Boolean = false
    }

    dedupeCache = CacheBuilder.newBuilder
      .expireAfterWrite(expiration.toMillis, TimeUnit.MILLISECONDS)
      .build(cacheLoader).asInstanceOf[LoadingCache[K, lang.Boolean]]
  }
}
