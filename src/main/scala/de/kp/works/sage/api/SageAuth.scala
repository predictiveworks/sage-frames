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

import com.google.gson.{JsonElement, JsonNull, JsonObject, JsonParser}
import de.kp.works.sage.conf.SageConf
import de.kp.works.sage.logging.Logging

/**
 * `SageAuth` is introduced to retrieve and manage the
 * access tokens, necessary to perform OAuth requests.
 */
object SageAuth extends Logging {

  val ACCESS_TOKEN_LIFESPAN: Int = 300 * 1000
  val REFRESH_TOKEN_LIFESPAN: Int = 2678400 * 1000
  /**
   * This parameter indicates the timestamp of the last
   * access token request. A token is valid for 5 minutes
   * and the respective refresh token for 31 days.
   *
   * This timestamp is used to determine when to refresh
   * the access token or request a completely new token.
   */
  private var lastAccessTime:Long = 0L
  /**
   * The actual token that is used to access the Sage API
   */
  private var accessToken:String = ""
  /**
   * The actual refresh token that is used to retrieve
   * another access token
   */
  private var refreshToken:String = ""
  /**
   * In case of a deployed `SageFrames` server, the file system
   * path to the configuration folder is provided as system
   * property `config.dir`
   */
  val folder: String = System.getProperty("config.dir")

  /**
   * The internal configuration is used, if the current
   * configuration is not set here
   */
  if (!SageConf.isInit) SageConf.init()

  private val sageCfg = SageConf.getSageCfg

  private val clientId = sageCfg.getString("clientId")
  private val clientSecret = sageCfg.getString("clientSecret")

  private val redirectUri =  sageCfg.getString("redirectUri")

  private val sageApi = new SageApi()
  /**
   * Initialize this authentication manager
   */
  init()
  /**
   * This internal method support the initialization
   * of the object
   */
  private def init():Unit = {
    if (lastAccessTime == 0L) read()
    getAccessToken
  }

  private def login():Unit = {

    val authToken = sageApi
      .login(clientId, clientSecret, redirectUri)

    if (authToken.isEmpty)
      throw new Exception(s"Retrieving access token failed.")

    lastAccessTime = System.currentTimeMillis()

    accessToken = authToken.get.accessToken
    refreshToken = authToken.get.refreshToken

  }

  private def refresh():Unit = {

    val authToken = sageApi
      .refreshToken(clientId, clientSecret, refreshToken)

    lastAccessTime = System.currentTimeMillis()

    accessToken = authToken.accessToken
    refreshToken = authToken.refreshToken

  }

  /**
   * This is the main method to expose the current access
   * token for other modules of the SageFrames projects.
   */
  def getAccessToken:Option[String] = {

    try {
      if (lastAccessTime == 0L) {
        /*
         * This is the first time the `SageFrames`
         * service has been started, and the access
         * token must be retrieved from the Sage API
         */
        login()
      }

      val now = System.currentTimeMillis()
      if ((now - lastAccessTime) > ACCESS_TOKEN_LIFESPAN) {
        /*
         * The current (or reloaded) access token is
         * no longer valid.
         */
        if ((now - lastAccessTime) > REFRESH_TOKEN_LIFESPAN) {
          /*
           * The current (or reloaded) refresh token
           * is also no longer valid, so re-login
           */
          login()
        }
        else {
          /*
           * The refresh token is still valid, so request
           * access token with refresh token
           */
          refresh()
        }
      }
      /*
       * Write authentication information to the local
       * file system
       */
      write()

      Some(accessToken)

    } catch {
      case t:Throwable =>
        error(s"Retrieving access token failed: ${t.getLocalizedMessage}")
        None
    }

  }
  /**
   * This method reads timestamp, access token and
   * refresh token from the local file system.
   */
  private def read():Unit = {

    val authElem = readAuthFile()
    if (!authElem.isJsonNull) {

      val authObj = authElem.getAsJsonObject
      lastAccessTime = authObj.get("lastAccessTime").getAsLong

      accessToken = authObj.get("accessToken").getAsString
      refreshToken = authObj.get("refreshToken").getAsString
    }

  }
  /**
   * This method writes timestamp, access token and
   * refresh token to the local file system, to enable
   * a proper restart after a kill of the SageFrames
   * service.
   */
  private def write():Unit = {

    val authObj = new JsonObject
    authObj.addProperty("lastAccessTime", lastAccessTime)

    authObj.addProperty("accessToken", accessToken)
    authObj.addProperty("refreshToken", refreshToken)

    writeAuthFile(authObj)

  }

  private def readAuthFile():JsonElement = {

    try {

      val authFile =
        if (folder == null) None else Some(s"$folder/authentication.json")

      val file = if (authFile.isEmpty) {
        val resource = getClass.getResource(s"/authentication.json")
        new java.io.File(resource.getFile)

      } else {
        new java.io.File(authFile.get)

      }

      val source = scala.io.Source.fromFile(file)
      val jsonElem = JsonParser.parseString(source.getLines.mkString)

      source.close
      jsonElem

    } catch {
      case _: Throwable => JsonNull.INSTANCE
    }

  }

  private def writeAuthFile(authObj:JsonObject):Unit = {

    try {

      val authFile =
        if (folder == null) None else Some(s"$folder/authentication.json")

      val file = if (authFile.isEmpty) {
        val resource = getClass.getResource(s"/authentication.json")
        new java.io.File(resource.getFile)

      } else {
        new java.io.File(authFile.get)

      }

      val overwrite = file.exists()
      val fileWriter =
        if (overwrite) new java.io.FileWriter(file, false)
        else new java.io.FileWriter(file)

      fileWriter.write(authObj.toString)

      fileWriter.flush()
      fileWriter.close()

    } catch {
      case _:Throwable =>
    }

  }

}
