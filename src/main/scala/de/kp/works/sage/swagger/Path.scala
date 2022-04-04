package de.kp.works.sage.swagger

import com.google.gson.JsonObject

import scala.collection.JavaConversions.{asScalaSet, iterableAsScalaIterable}
import scala.collection.mutable

case class SagePath(
  endpoint:String,
  method:String
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

          sagePaths += SagePath(endpoint = endpoint, method = "delete")

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

          sagePaths += SagePath(endpoint = endpoint, method = "get")

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

          sagePaths += SagePath(endpoint = endpoint, method = "post")

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

          sagePaths += SagePath(endpoint = endpoint, method = "put")

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
