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
