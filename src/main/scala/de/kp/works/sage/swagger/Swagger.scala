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
import com.google.gson.{JsonObject, JsonParser}
import scala.io.Source

trait Swagger {

  val swagger: JsonObject = file2Json

  def getRefName(json:JsonObject):String = {
    json.get("$ref").getAsString.split("/").last
  }

  def file2Json:JsonObject = {

    val resource = getClass.getResource("/swagger.full.json")

    val source = Source.fromFile(resource.getFile)
    val lines = source.getLines

    val str = lines.mkString
    source.close

    val swagger = JsonParser.parseString(str)
    swagger.getAsJsonObject

  }

}
