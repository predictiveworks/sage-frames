package de.kp.works.sage.api

import com.google.gson.JsonElement
import de.kp.works.sage.logging.Logging
import de.kp.works.sage.spark.Session
import de.kp.works.sage.swagger.Paths
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConversions.iterableAsScalaIterable

object SageReader {

  def main(args:Array[String]):Unit = {

    val endpoint = "/purchase_invoices"
    val reader = new SageReader("")

    reader.read(endpoint, Map.empty[String,Any])

  }
}

class SageReader(accessToken:String) extends Logging {

  private val session = Session.getSession
  import session.implicits._
  /*+
   * The number of items returned with a page request
   * is set to 200. This is a fixed value and cannot
   * be changed
   */
  val itemsPerPage = s"items_per_page=200"

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
        if (paging) doPageGet(endpoint) else doGet(endpoint)

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

  private def doGet(endpoint:String):DataFrame = {

    info(s"Perform GET for `$endpoint`.")
    ???
  }

  private def doPageGet(endpoint:String):DataFrame = {

    info(s"Perform paged GET for `$endpoint`.")
    /*
     * The current implementation leverages the maximum
     * items per page as specified by the Sage API.
     */
    var requestUrl = endpoint + "&" + itemsPerPage
    ???
  }
  /**
   * A helper method to assign the provided
   * `accessToken` to the request header.
   */
  private def getHeaders:Map[String,String] = {
    val headers = Map("Authorization" -> s"Bearer $accessToken")
    headers
  }

  private def json2DataFrame(json: JsonElement): DataFrame = {

    val seq = if (json.isJsonArray) {
      /*
       * Transform response JSON array into a dataset
       * that is transformed afterwards into a `DataFrame`
       */
      json.getAsJsonArray.map(e => e.toString).toSeq

    } else if (json.isJsonObject) {
      /*
       * Transform response JSON object into a dataset
       * that is transformed afterwards into a `DataFrame`
       */
      json.toString :: Nil

    } else
      Seq.empty[String]

    if (seq.isEmpty) session.emptyDataFrame
    else {

      val dataset = session.createDataset(seq)
      val dataframe = session.read.json(dataset)

      dataframe

    }

  }

}
