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

package com.orgsync.oskr.events.streams.delivery

import java.io.StringWriter
import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.databind.ObjectMapper
import com.github.jknack.handlebars._
import com.github.jknack.handlebars.cache.GuavaTemplateCache
import com.github.jknack.handlebars.context.MapValueResolver
import com.github.jknack.handlebars.io.TemplateSource
import com.google.common.cache.CacheBuilder
import com.mitchellbosecke.pebble.PebbleEngine
import com.mitchellbosecke.pebble.loader.StringLoader
import com.mitchellbosecke.pebble.template.PebbleTemplate
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonAST.JObject

class TemplateCache {
  private val jsonObjectMapper = new ObjectMapper

  private val handlebarsCache = CacheBuilder
    .newBuilder
    .expireAfterAccess(1, TimeUnit.HOURS)
    .maximumSize(1000)
    .build[TemplateSource, Template]

  private val handlebars = new Handlebars()
    .`with`(new GuavaTemplateCache(handlebarsCache))

  def renderHandlebars(template: String, context: JObject): String = {
    val node = asJsonNode(context)
    val json = Context.newBuilder(node).resolver(
      JsonNodeValueResolver.INSTANCE,
      MapValueResolver.INSTANCE
    ).build
    val compiled = handlebars.compileInline(template)

    compiled.apply(json)
  }

  private val pebbleCache = CacheBuilder
    .newBuilder
    .expireAfterAccess(1, TimeUnit.HOURS)
    .maximumSize(1000)
    .build[AnyRef, PebbleTemplate]

  private val pebbleEngine = new PebbleEngine.Builder()
    .templateCache(pebbleCache)
    .loader(new StringLoader)
    .build

  def renderPebble(template: String, context: JObject): String = {
    implicit val formats = DefaultFormats
    val writer = new StringWriter
    val compiled = pebbleEngine.getTemplate(template)
    val node = asJsonNode(context)
    val m: java.util.Map[String, Object] = jsonObjectMapper.convertValue(
      node, classOf[java.util.Map[String, Object]]
    )

    compiled.evaluate(writer, m)
    writer.toString
  }
}
