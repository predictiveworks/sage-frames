package de.kp.works.sage.swagger

import com.google.gson.{JsonObject, JsonParser}
import scala.io.Source

trait Swagger {

  val swagger: JsonObject = file2Json

  def getRefName(json:JsonObject):String = {
    json.get("$ref").getAsString.split("/").last
  }

  def file2Json:JsonObject = {

    val source = Source.fromFile("src/main/resources/swagger.full.json")
    val lines = source.getLines

    val str = lines.mkString
    source.close

    val swagger = JsonParser.parseString(str)
    swagger.getAsJsonObject

  }

}
