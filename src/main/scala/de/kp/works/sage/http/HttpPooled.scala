package de.kp.works.sage.http

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

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, HttpResponse, StatusCodes}
import akka.http.scaladsl.model.headers.RawHeader
import akka.stream.{ActorMaterializer, KillSwitches}
import akka.stream.scaladsl.{RestartSource, Sink, Source}
import akka.util.{ByteString, Timeout}

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.{Failure, Success, Try}

trait HttpPooled {

  private val uuid = java.util.UUID.randomUUID.toString
  implicit val httpSystem: ActorSystem = ActorSystem(s"http-pooled-$uuid")

  implicit lazy val httpContext: ExecutionContextExecutor = httpSystem.dispatcher
  implicit val httpMaterializer: ActorMaterializer = ActorMaterializer()

  /**
   * Common timeout for all Akka connection
   */
  private val duration: FiniteDuration = 10.seconds
  implicit val timeout: Timeout = Timeout(duration)
  /**
   * Leveraging `superPool`, you get back a Flow which will make sure
   * that it will not take more requests than the pool is able to handle,
   * because of the backpressure.
   */
  private val pool = Http(httpSystem).superPool[NotUsed]()
  /**
   * A shared kill switch used to terminate the *entire stream* if needed
   */
  private val killSwitch = KillSwitches.shared("killSwitch")
  /**
   * This method supports GET requests with `backoff` support,
   * and also enables proper handling of `Too many requests`.
   *
   * The [Backoff] must be adjusted to the selected REST API
   * like Shopify, Sage, (Open)Weather etc.
   */
  def singleGet(endpoint:String, headers:Map[String,String], backoff:BackoffPolicy, parallelism:Int = 1):Future[Source[ByteString,Any]] = {

    /*
     * STEP #1: Build HTTP request from the provided endpoint
     * and headers
     */
    val request = {
      if (headers.isEmpty)
        HttpRequest(HttpMethods.GET, endpoint)

      else
        HttpRequest(HttpMethods.GET, endpoint, headers=headers.map{case(k,v) => RawHeader(k, v)}.toList)

    }
    /*
     * STEP #2: Perform connection pool based Http request; it uses
     * the `backoff` feature to supervise a stream and restart it with
     * exponential backoff.
     */
    RestartSource.withBackoff(
      minBackoff = backoff.minBackoff, maxBackoff = backoff.maxBackoff, randomFactor = backoff.randomFactor, maxRestarts = backoff.maxRestarts)
      { () =>
        Source.single((request, NotUsed)).via(pool)
          .mapAsync(parallelism = parallelism)(handleResponse)

      }.runWith(Sink.head).recover {
        case _ => throw FailedAfterMaxRetries
      }

  }

  val handleResponse:PartialFunction[(Try[HttpResponse],NotUsed), Future[Source[ByteString,Any]]] =  {
    case (Success(answer), _) => answer match {
      /*
       * Handling regular Http responses
       */
      case HttpResponse(StatusCodes.OK, _, entity, _) =>
        Future {entity.withoutSizeLimit().dataBytes}

      case HttpResponse(StatusCodes.UnprocessableEntity,_,entity,_) =>
        Future {entity.dataBytes}
      /*
       * Handling (429) Too many requests, and apply
       * exponential backoff
       */
      case HttpResponse(StatusCodes.TooManyRequests,_,entity,_) =>
        entity.discardBytes()
        throw TooManyRequests

      case HttpResponse(statusCode, _, entity, _) =>
        entity.discardBytes()
        killSwitch.shutdown()

        throw new Exception(s"Request to Http endpoint returns with: ${statusCode.value}.")
    }
    case (Failure(failure), _) => throw failure

  }

}
