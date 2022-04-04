package de.kp.works.sage.api

import com.google.gson.JsonElement
import de.kp.works.sage.spark.Session
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConversions.iterableAsScalaIterable

class SageReader(accessToken:String) {

  private val session = Session.getSession
  import session.implicits._

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
