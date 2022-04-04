package de.kp.works.sage.swagger

/**
 * Copyright (c) 2019 - 2022 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 *
 */

import com.google.gson.JsonObject

import scala.collection.JavaConversions.{asScalaSet, iterableAsScalaIterable}
import scala.collection.mutable

case class SagePath(
  /*
   * The Sage endpoint as defined in the
   * Swagger file.
   */
  endpoint:String,
  /*
   * The HTTP method as defined in the
   * Swagger file
   */
  method:String,
  /*
   * The name of the response schema as
   * defined in the Swagger file
   */
  schemaName:String,
  /*
   * The schema type as defined in the
   * Swagger file. Supported values are
   * `array` and `object`.
   */
  schemaType:String
)

object Path extends Swagger {

  val ignore = Seq("x-sage-pathtitle", "x-sage-changelog")

  def buildPaths():Unit = {

    val sagePaths = mutable.ArrayBuffer.empty[SagePath]
    val paths = swagger.get("paths").getAsJsonObject

    val endpoints = paths.keySet()
    endpoints.foreach(endpoint => {

      val path = paths.get(endpoint).getAsJsonObject
      val methods = path.keySet().filter(key => !ignore.contains(key))
      /*
       * Support http methods: delete, get, post, put
       */
      methods.foreach {
        case "delete" =>
          val request = getJsonRequest(path, "delete")
          /*
           * Extract parameters for the `delete` request
           */
          val parameters = request.get("parameters").getAsJsonArray
          /*
           * Extract responses for the `delete` request
           */
          val responses = request.get("responses").getAsJsonObject
          val code = responses.keySet().head
          /*
           * Extract response schema for the `delete` request
           */
          val schemaName = ""
          val schemaType = ""

          sagePaths += SagePath(
            endpoint = endpoint, method = "delete", schemaName, schemaType)

        case "get" =>
          val request = getJsonRequest(path, "get")
          /*
           * Extract parameters for the `get` request
           */
          val parameters = request.get("parameters").getAsJsonArray
          /*
           * Extract responses for the `get` request
           */
          val responses = request.get("responses").getAsJsonObject
          val code = responses.keySet().head
          /*
           * Extract response schema for the `get` request
           */
          val schema = responses.get(code).getAsJsonObject
            .get("schema").getAsJsonObject

          var schemaName = ""
          var schemaType = ""

          if (schema.has("$ref")) {
            schemaName = getRefName(schema)
            schemaType = "object"

          }

          sagePaths += SagePath(
            endpoint = endpoint, method = "get", schemaName, schemaType)

        case "post" =>
          val request = getJsonRequest(path, "post")
          /*
           * Extract parameters for the `post` request
           */
          val parameters = request.get("parameters").getAsJsonArray
          /*
           * Extract responses for the `post` request
           */
          val responses = request.get("responses").getAsJsonObject
          val code = responses.keySet().head
          /*
           * Extract response schema for the `delete` request
           */
          val schema = responses.get(code).getAsJsonObject
            .get("schema").getAsJsonObject

          var schemaName = ""
          var schemaType = ""

          if (schema.has("$ref")) {
            schemaName = getRefName(schema)
            schemaType = "object"

          }

          sagePaths += SagePath(
            endpoint = endpoint, method = "post", schemaName, schemaType)

        case "put" =>
          val request = getJsonRequest(path, "put")
          /*
           * Extract parameters for the `put` request
           */
          val parameters = request.get("parameters").getAsJsonArray
          /*
           * Extract responses for the `put` request
           */
          val responses = request.get("responses").getAsJsonObject
          val code = responses.keySet().head
          println(code)
          /*
           * Extract response schema for the `delete` request
           */
          val schema = responses.get(code).getAsJsonObject
            .get("schema").getAsJsonObject

          var schemaName = ""
          var schemaType = ""

          if (schema.has("$ref")) {
            schemaName = getRefName(schema)
            schemaType = "object"

          }

          sagePaths += SagePath(
            endpoint = endpoint, method = "put", schemaName, schemaType)

        case method => throw new Exception(s"Http method `$method` is not supported.")
      }

    })
  }

  def getJsonRequest(path:JsonObject, method:String):JsonObject = {

    val request = path.get(method).getAsJsonObject
    val contentType = request.get("produces")
      .getAsJsonArray.head.getAsString

    assert(contentType == "application/json" || contentType.isEmpty)
    request

  }
}
