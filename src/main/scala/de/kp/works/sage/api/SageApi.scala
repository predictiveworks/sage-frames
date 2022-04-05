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

import com.google.gson.JsonObject
import de.kp.works.sage.conf.SageConf
import de.kp.works.sage.http.HttpConnect
import de.kp.works.sage.logging.Logging
import org.openqa.selenium.By
import org.openqa.selenium.support.ui.{ExpectedConditions, WebDriverWait}

import java.net.{URI, URLDecoder}
import java.time.Duration
import scala.collection.JavaConversions.iterableAsScalaIterable

case class AuthToken(
  accessToken:String,
  accessTokenExpiresIn: Int,
  refreshToken:String,
  refreshTokenExpiresIn: Int
)

object SageApi {

  /**
   * The internal configuration is used, if the current
   * configuration is not set here
   */
  if (!SageConf.isInit) SageConf.init()

  private val sageCfg = SageConf.getSageCfg

  private val clientId = sageCfg.getString("clientId")
  private val clientSecret = sageCfg.getString("clientSecret")

  private val redirectUri =  sageCfg.getString("redirectUri")
  private val api = new SageApi()

  def main(args: Array[String]): Unit = {

    api.getAuthCode(clientId, redirectUri)
    System.exit(0)

  }

  def login():Option[AuthToken] = {
    api.loginRequest(clientId, clientSecret, redirectUri)
  }

}

/**
 * The `SageApi` provides a low-level access to the
 * Sage cloud platform and connects the result to
 * the world of `DataFrame`s.
 *
 * The validation of the DataFrame, post-processing
 * and writing the frame to a database managed by the
 * SageReader.
 */
class SageApi extends HttpConnect with Logging {
  /**
   * This Sage authentication endpoint returns
   * the authorization code that is needed to
   * retrieve the (final) authorization token
   * in a subsequent request within 60 seconds
   */
  private val authCodeUrl = "https://www.sageone.com/oauth2/auth/central?filter=apiv3.1&response_type=code&scope=full_access"
  /**
   * This Sage authentication endpoint is used
   * to retrieve and refresh the authorization
   * token.
   */
  private val authTokenUrl = "https://oauth.accounting.sage.com/token"

  /*
  * Note, the `redirectUri` should be escaped; the `state` parameter
  * is a String used to protect against CSRF attacks. Although state
  * is optional, we recommend including this.
  *
  * This should be a string that cannot be guessed. Note: If the value
  * of the state parameter returned in the response does not match the
  * state that was provided in the original request, it does not originate
  * from the original request and you must not continue.
  */
  def loginRequest(clientId: String, clientSecret: String, redirectUri: String): Option[AuthToken] = {
    /*
     * STEP #1: Retrieve the authorization code
     *
     * The authorization code obtained above is for
     * single-use only and expires after 60 seconds.
     */
    val authCode = getAuthCode(clientId, redirectUri)
    if (authCode.isEmpty) {
      error(s"Retrieving authentication code failed.")
      None

    } else {
      /*
       * STEP #2: Retrieve the access token
       *
       * The access token expires after 5 minutes and
       * can be refreshed with the `refresh_token` within
       * 31 days
       */
      val authToken = getAuthToken(clientId, clientSecret, redirectUri, authCode.get)
      Some(authToken)

    }
  }
  /**
   * https://developer-community.sage.com/topic/235-using-the-sage-api-sdk/
   *
   * At this time the initial authentication requires a user to input credentials
   * to successfully log in, then authorise an application to access the business.
   *
   * Once this has been done, it's then possible to utilise the refresh token to
   * effectively keep the connection alive.
   *
   * The user interaction is simulated by leveraging a headless Chrome combined
   * with Selenium
   */
  def getAuthCode(clientId: String, redirectUri: String): Option[String] = {

    val state = java.util.UUID.randomUUID().toString
    val endpoint = s"$authCodeUrl&client_id=$clientId&redirect_uri=$redirectUri&state=$state"

    val browser = SageChrome.chromeDriver
    browser.get(endpoint)
    /*
     * The web page, retrieved as a response, requires to
     * select a certain country: the current implementation
     * leverages German as language = "de"
     */
    val languageBtn = browser
      .findElement(By.xpath("//li[@id='de']"))
      .findElement(By.xpath("//button"))
    /*
     * Click on the "de" button; this should open the login
     * page, where we assign `email` and `password`
     */
    languageBtn.click()
    /*
     * The implementation of `getAuthCode` assumes that the user
     * has not logged in to the Sage Integration system prior to
     * requesting an Authorization Code.
     *
     * The user will be redirected to the Login Page. The user
     * needs to input a username (email) and password for the
     * company admin and select Log In.
     *
     * Note, the web page returned after `click` is generated
     * via javascript; this implies that the Chrome driver
     * cannot be used directly to send email & password.
     *
     * Approach: Wait until DOM element id="login-page" is loaded
     */
    val complete = ExpectedConditions.presenceOfElementLocated(By.id("login-page"))
    /*
     * Wait 30 seconds until the original web page is dynamically
     * built, and then set `email` and `password`.
     */
    val login = new WebDriverWait(browser, Duration.ofSeconds(30)).until(complete)

    val email = login.findElement(By.xpath("//input[type='email'"))
    email.sendKeys("")

    val password = login.findElement(By.xpath("//input[type='password'"))
    password.sendKeys("")

    val loginBtn = browser.findElement(By.xpath("//button[name='submit'"))
    loginBtn.click()
    /*
     * The flow, retrieving the authentication code, continues with the
     * authorization Page. This page will show what access you will have
     * to their accounts.
     */

    // TODO Confirm your access rights

    /*
     * As a final step, the flow returns to the `redirectUri`, and exposes
     * the authentication code as part of the uri. Sample:
     *
     * https://exampleapp.com/callback?code=Yzk5ZDczMzRlNDEwY&state=5ca75bd30
     */
    val uri = new URI(browser.getCurrentUrl)
    /*
     * Extract `code` from URI
     */
    val authCode = {

      val queryParams = getQueryParams(uri.getQuery)
      if (queryParams.contains("code")) {

        val params = queryParams("code")
        if (params.isEmpty) None
        else {

          params.head

        }
      } else None

    }

    browser.close()
    authCode

  }

  private def getQueryParams(query: String): Map[String, Seq[Option[String]]] = {

    case class Param(key: String, value: Option[String])

    def separate(param: String): Param = {

      param.split("=")
        .map(e => URLDecoder.decode(e, "UTF-8")) match {
        case Array(key, value) => Param(key, Some(value))
        case Array(key) => Param(key, None)
      }

    }

    val result = query.split("&").toSeq
      .map(param => separate(param))
      .groupBy(param => param.key)
      .mapValues(values => values.map(p => p.value))

    result

  }

  def getAuthToken(clientId: String, clientSecret: String, redirectUri: String, authCode: String): AuthToken = {

    val endpoint = authTokenUrl
    val headers = Map.empty[String, String]

    val body = new JsonObject
    body.addProperty("client_id", clientId)

    body.addProperty("client_secret", clientSecret)
    body.addProperty("code", authCode)

    body.addProperty("grant_type", "authorization_code")
    body.addProperty("redirect_uri", redirectUri)

    val bytes = post(endpoint, headers, body, "application/x-www-form-urlencoded")
    val json = extractJsonBody(bytes).getAsJsonObject
    /*
     * The access token expires after 5 minutes (300 seconds) and
     * can be refreshed with the `refresh_token` within 31 days
     * {
     * "access_token": "...",
     * "scopes": "full_access",
     * "token_type": "bearer",
     * "expires_in": 300,
     * "refresh_token": "...",
     * "refresh_token_expires_in": 2678400,
     * "requested_by_id": "..."
     * }
     */
    val access_token = json.get("access_token").getAsString
    val expires_in = json.get("expires_in").getAsInt

    val refresh_token = json.get("refresh_token").getAsString
    val refresh_token_expires_in = json.get("refresh_token_expires_in").getAsInt

    AuthToken(access_token, expires_in, refresh_token, refresh_token_expires_in)

  }

  def refreshToken(clientId: String, clientSecret: String, refreshToken: String): AuthToken = {

    val endpoint = authTokenUrl
    val headers = Map.empty[String, String]

    val body = new JsonObject
    body.addProperty("client_id", clientId)

    body.addProperty("client_secret", clientSecret)
    body.addProperty("refresh_token", refreshToken)

    body.addProperty("grant_type", "refresh_token")

    val bytes = post(endpoint, headers, body, "application/x-www-form-urlencoded")
    val json = extractJsonBody(bytes).getAsJsonObject
    /*
     * The access token expires after 5 minutes (300 seconds) and
     * can be refreshed with the `refresh_token` within 31 days
     * {
     * "access_token": "...",
     * "scopes": "full_access",
     * "token_type": "bearer",
     * "expires_in": 300,
     * "refresh_token": "...",
     * "refresh_token_expires_in": 2678400,
     * "requested_by_id": "..."
     * }
     */
    val access_token = json.get("access_token").getAsString
    val expires_in = json.get("expires_in").getAsInt

    val refresh_token = json.get("refresh_token").getAsString
    val refresh_token_expires_in = json.get("refresh_token_expires_in").getAsInt

    AuthToken(access_token, expires_in, refresh_token, refresh_token_expires_in)

  }

  def getRequest(endpoint: String, headers: Map[String, String], responseType:String): Seq[String] = {

    try {

      val bytes = get(endpoint, headers)
      val json = extractJsonBody(bytes)

      if (responseType == "object") {

        if (!json.isJsonObject)
          throw new Exception(s"Response does not match type `$responseType`.")

        Seq(json.toString)

      } else if (responseType == "array") {

        if (!json.isJsonArray)
          throw new Exception(s"Response does not match type `$responseType`.")

        json.getAsJsonArray.map(e => e.toString).toSeq

      } else
        throw new Exception(s"Unknown response type `$responseType` detected.")

    } catch {
      case t: Throwable =>
        error(s"GET for `$endpoint` failed: ${t.getLocalizedMessage}")
        Seq.empty[String]
    }

  }
  /**
   * This method performs a paginated Http `GET` request
   * to the Sage endpoint. It is controlled by the query
   * parameters, `items_per_page` and `page`.
   *
   * In case of a paginated request, the response slightly differs from
   * the API reference documentation.
   */
  def getPageRequest(endpoint: String, headers: Map[String, String]): (Int, Seq[String]) = {

    try {

      val bytes = get(endpoint, headers)
      val json = extractJsonBody(bytes)

      if (json == null) (0, Seq.empty[String])
      else {
        /*
         * Pagination sample: GET sales_quotes?page=2&items_per_page=25
         *
         * Response:
         * {
         *    "$total": 97,
         *    "$page": 2,
         *    "$next": "/sales_quotes?page=3&items_per_page=25",
         *    "$back": "/sales_quotes?page=1&items_per_page=25",
         *    "$itemsPerPage": 25,
         *    "$items": [
         *      {
         *        "id": "401aa0e1424242ea98de55608dacf869",
         *        "displayed_as": "SQ-2",
         *        "$path": "/sales_quotes/401aa0e1424242ea98de55608dacf869"
         *      },
         *      ...
         *    ]
         * }
         */
        val jsonObj = json.getAsJsonObject
        val total = jsonObj.get("$total").getAsInt

        val items = jsonObj.get("$items").getAsJsonArray
          .map(e => e.toString)
          .toSeq

        (total, items)

      }

    } catch {
      case t: Throwable =>
        error(s"Paged GET for `$endpoint` failed: ${t.getLocalizedMessage}")
        (0, Seq.empty[String])

    }

  }

}
