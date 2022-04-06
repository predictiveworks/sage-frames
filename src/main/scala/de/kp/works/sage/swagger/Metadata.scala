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

import org.apache.spark.sql.types.{StructField, StructType}

case class SageSchema(schemaName:String, schemaType:String)

object Metadata {
  /**
   * This method retrieves the `table names` extracted
   * from the Sage Swagger file, that can be used with
   * read operations.
   */
  def readSchemas:Seq[SageSchema] = {

    val readPaths = Paths.getReadPaths
    readPaths.map(path => {
      SageSchema(path.schemaName, path.schemaType)
    })

  }

  def getReadSchema(schemaName:String, flatten:Boolean):Option[StructType] = {

    val readSchema = Schemas.getSchema(schemaName)
    if (readSchema.isEmpty) None
    else {

      if (flatten) {
        val flattenedSchema = StructType(flattenFields(readSchema.get))
        Some(flattenedSchema)

      }
      else
        readSchema
    }

  }

  def flattenFields(schema: StructType, prefix: String = null) : Array[StructField] = {
    schema.fields.flatMap(f => {
      val columnName = if (prefix == null) f.name else (prefix + "." + f.name)

      f.dataType match {
        case st: StructType => flattenFields(st, columnName)
        case _ => {

          val fieldName = columnName.replace(".","_")
          val fieldType = f.dataType
          val fieldNullable = f.nullable

          Array(StructField(fieldName, fieldType, fieldNullable))

        }
      }
    })
  }

}
