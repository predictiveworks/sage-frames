package de.kp.works.sage.api

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

import de.kp.works.sage.logging.Logging
import de.kp.works.sage.spark.Session
import de.kp.works.sage.swagger.{Paths, SagePath}
import org.apache.spark.sql.DataFrame

object SageReader {

  def main(args:Array[String]):Unit = {

    val endpoint = "/purchase_invoices"
    val reader = new SageReader("")

    reader.read(endpoint, Map.empty[String,Any])

  }
}

class SageReader(accessToken:String) extends Logging {
  /**
   * Reference to the Sage HTTP API
   */
  private val sageApi = new SageApi()
  /**
   * Spark session to enable the use of DataFrames
   */
  private val session = Session.getSession
  import session.implicits._
  /**
   * The number of items returned with a page request
   * is set to 200. This is a fixed value and cannot
   * be changed
   */
  private val itemsPerPage = s"items_per_page=200"

  def read(path:String, params:Map[String, Any]):DataFrame = {

    try {
      /*
       * STEP #1: Retrieve Swagger based definition of the
       * provided path
       */
      val readPath = Paths.getReadPath(path)
      if (readPath.nonEmpty) {
        /*
         * STEP #2: Check whether the provided path contains
         * `{key}` and if, replace by provided parameters
         */
        var endpoint = if (path.contains("{key}")) {
          if (params.contains("key")) {
            /*
             * The current implementation expects
             * keys to be [String]s
             */
            val key = params("key").asInstanceOf[String]
            path.replace("{key}", key)

          } else
            throw new Exception(s"Parameters do not contain `key`.")

        } else path

        val queryParams = readPath.get.queryParams
        /*
         * STEP #3: Check whether all required query parameters
         * are provided and built the query path of the request
         * url
         */
        val requiredParams = readPath.get.queryParams
          .filter(param => param.required)
          .map(_.paramName)

        var complete = true
        requiredParams.foreach(paramName => {
          if (!params.contains(paramName)) complete = false
        })

        if (!complete)
          throw new Exception(s"Required parameters are missing.")
        /*
         * Build parameter lookup
         */
        val lookup = queryParams
          .map(p => (p.paramName, p)).toMap

        val ignores = Seq("items_per_page", "page")
        val query = params
          /*
           * Paging parameters are controlled by the
           * `SageReader` and are ignored as this
           * stage
           */
          .filter{case(k, _) => !ignores.contains(k)}
          .map{case(k,v) =>
            val param = lookup(k)
            param.paramType match {
              case "boolean" =>
                s"$k=${v.asInstanceOf[Boolean]}"

              case "integer" =>
                s"$k=${v.asInstanceOf[Int]}"

              case "string" =>
                s"$k=${v.asInstanceOf[String]}"

              case _ =>
                throw new Exception(s"Data type `${param.paramType}` is not supported.")
            }
          }.toSeq.mkString("&")

        endpoint =
          if (query.nonEmpty) endpoint + "?" + query else endpoint

        /*
         * STEP #4: Check whether the access to the endpoint
         * is a paging request or not, and access Sage API.
         */
        val paging = lookup.contains("items_per_page") || lookup.contains("page")
        if (paging) doPageGet(endpoint, readPath.get) else doGet(endpoint, readPath.get)

      } else {
        warn(s"The provided endpoint `$path` is not supported.")
        session.emptyDataFrame
      }

    } catch {
      case t:Throwable =>
        error(s"Reading from endpoint `$path` failed: ${t.getMessage}")
        session.emptyDataFrame
    }

  }

  private def doGet(endpoint:String, sagePath:SagePath):DataFrame = {

    info(s"Perform GET for `$endpoint`.")

    val headers = getHeaders
    val responseType = sagePath.schemaType

    val rows = sageApi.getRequest(endpoint, headers, responseType)
    /*
     * Transform the final result into a `DataFrame`
     * to ease Spark based data computing
     */
    if (rows.isEmpty) session.emptyDataFrame
    else {
      val dataset = session.createDataset(rows)
      session.read.json(dataset)

    }

  }

  private def doPageGet(endpoint:String, sagePath:SagePath):DataFrame = {
    /*
     * Paging requests expect that the Sage API always returns
     * an `array`; therefore, there is no explicit need to evaluate
     * the associated `sagePath`.
     */
    info(s"Perform paged GET for `$endpoint`.")
    /*
     */
    var nextPage = 1
    /*
     * The current implementation leverages the maximum
     * items per page as specified by the Sage API.
     */
    val requestUrl = endpoint + "&" + itemsPerPage

    var rows = Seq.empty[String]
    val headers = getHeaders
    /*
     * The current implementation takes the number of
     * returned items as a decision criteria whether
     * to continue requesting the API or not
     */
    var hasNext = true
    while (hasNext) {
      /*
       * Access Sage API with the current paging
       */
      val nextPageUrl = requestUrl + "&page=" + nextPage
      val (_, items) = sageApi.getPageRequest(nextPageUrl, headers)
      /*
       * Register the retrieved items and and
       * determine whether to invoke the next
       * request
       */
      rows = rows ++ items
      if (items.size < 200)
        hasNext = false

      else
        nextPage += 1

      Thread.sleep(200)

    }
    /*
     * Transform the final result into a `DataFrame`
     * to ease Spark based data computing
     */
    if (rows.isEmpty) session.emptyDataFrame
    else {
      val dataset = session.createDataset(rows)
      session.read.json(dataset)

    }

  }
  /**
   * A helper method to assign the provided
   * `accessToken` to the request header.
   */
  private def getHeaders:Map[String,String] = {
    val headers = Map("Authorization" -> s"Bearer $accessToken")
    headers
  }

}
