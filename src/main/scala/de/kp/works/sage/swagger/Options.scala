package de.kp.works.sage.swagger

import scala.collection.JavaConversions.iterableAsScalaIterable

object Options extends Swagger {

  def getPath:String = {
    val host = swagger.get("host").getAsString
    val basePath = swagger.get("basePath").getAsString

    val schemes = swagger.get("schemes")
      .getAsJsonArray
      .map(e => e.getAsString)

    val path = s"${schemes.head}://$host$basePath"
    path

  }

}
