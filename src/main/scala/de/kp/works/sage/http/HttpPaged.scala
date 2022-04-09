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
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, HttpResponse, StatusCodes}
import akka.stream.scaladsl.{RestartSource, Sink, Source}
import akka.stream.stage._
import akka.stream.{Attributes, KillSwitches, Outlet, SourceShape}
import akka.util.ByteString

import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success, Try}

trait PageHandler {
  /**
   * Query parameters and variables to control
   * paged requests for a certain endpoint
   */
  protected var page:Option[Int] = None
  protected var pageParam:Option[String] = None

  protected var pageSize:Option[Int] = None
  protected var pageSizeParam:Option[String] = None
  /**
   * The [HttpResponse] of the last request;
   * it can be used to determine whether the
   * paged request is completed
   */
  protected var httpResponse:Option[HttpResponse] = None
  /**
   * Public method that determines whether the
   * paged HTTP request has reached its end
   */
  def isComplete:Boolean
  /**
   * A public method to specify the current
   * page
   */
  def setPage(number:Int):PageHandler = {
    page = Some(number)
    this
  }
  /**
   * A public method to describe the name of
   * the page parameter
   */
  def setPageParam(name:String):PageHandler = {
    pageParam = Some(name)
    this
  }
  /**
   * A public method to determine the amount
   * of items per page request
   */
  def setPageSize(limit:Int):PageHandler = {
    pageSize = Some(limit)
    this
  }
  /**
   * A public method to describe the name of
   * the page size parameter
   */
  def setPageSizeParam(name:String):PageHandler = {
    pageSizeParam = Some(name)
    this
  }
  /**
   * A public method to register the response
   * of the last HTTP request
   */
  def setHttpResponse(response:HttpResponse):PageHandler = {
    httpResponse = Some(response)
    this
  }
  /**
   * A public method to retrieve the next page
   * uri; the default implementation is based
   * on the specified query parameters
   */
  def getNextUri(endpoint:String):String = {

    val queryPart = getQueryPart
    val uri =
      if (endpoint.contains("&")) s"$endpoint&$queryPart" else s"$endpoint?$queryPart"

    uri

  }
  /**
   * A public method to retrieve the next
   * query part for the paged HTTP request
   */
  private def getQueryPart:String = {

    if (page.isEmpty)
      throw new Exception("No `page` value provided.")

    if (pageParam.isEmpty)
      throw new Exception("No `page` parameter provided.")

    if (pageSize.isEmpty)
      throw new Exception("No `page size` value provided.")

    if (pageSizeParam.isEmpty)
      throw new Exception("No `page size` parameter provided.")

    /* STEP #1: Increase the current page */
    page = Some(page.get + 1)

    /*
     * STEP #2: Build `query`part
     */
    val queryPart = Seq(
      s"${pageParam.get}=${page.get}",
      s"${pageSizeParam.get}=${pageSize.get}").mkString("&")

    queryPart
  }

}
object HttpPaged {

  def apply(
    endpoint:String,
    headers:Map[String,String],
    backoff:BackoffPolicy,
    handler:PageHandler)
   (implicit httpSystem: ActorSystem, httpContext: ExecutionContextExecutor): Source[String, NotUsed] = Source
      .fromGraph(new HttpPaged(endpoint, headers, backoff, handler))

}

class HttpPaged(
  /*
   * The HTTP endpoint
   */
  endpoint:String,
  /*
   * HTTP headers, e.g., to send authentication
   * information to the selected REST endpoint
   */
  headers:Map[String,String],
  /*
   * The Backoff policy used for exponential
   * backoff for each HTTP request
   */
  backoff:BackoffPolicy,
  /*
   * The page handler determines the next page
   * also decides whether the paging requests
   * are done
   */
  handler: PageHandler) (implicit httpSystem: ActorSystem, httpContext: ExecutionContextExecutor)
  extends GraphStage[SourceShape[String]] {
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
   * This source returns a JSON or Text String
   */
  val out: Outlet[String] = Outlet("HttpPaged.out")

  override val shape: SourceShape[String] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new HttpPagedLogic()

  private class HttpPagedLogic extends GraphStageLogic(shape) with OutHandler with StageLogging {
    /*
     * Buffer that contains the items returned by
     * the series of HTTP requests
     */
    private var buffer:List[String] = List.empty[String]
    /*
     * Flag to control that not more than one
     * request at a time is performed
     */
    private val pending = new AtomicBoolean(false)
    /*
     * This is a relevant approach to make things
     * more generic
     */
    private var callback: AsyncCallback[Try[Source[ByteString, Any]]] = _

    setHandler(out, this)

    override def preStart(): Unit = {
      callback = getAsyncCallback[Try[Source[ByteString, Any]]](tryPushAfterResponse)
      readNextPage()
    }

    private def tryPushAfterResponse(result: Try[Source[ByteString, Any]]): Unit = result match {
      case Success(source) =>
        /*
         * Make sure that the next request is not blocked
         */
        pending.set(false)
        /*
         * Extract body as String from request entity
         */
        source
          .runFold(ByteString(""))(_ ++ _)
          .foreach(bytes => {
            val body = bytes.decodeString("UTF-8")
            /*
             * Assign the extracted body to the
             * internal buffer for subsequent
             * computing
             */
            if (buffer.isEmpty) {
              buffer = body :: Nil

            } else {
              buffer = buffer ++ (body :: Nil)
            }
            /*
             * Push the first entry of the buffer
             */
            if (isAvailable(out)) {
              push(out, buffer.head)
              buffer = buffer.tail
            }

            readNextPage()

          })

      case Failure(failure) => failStage(failure)

    }

    def readNextPage():Unit = {
      /*
       * Determine whether to perform the next HTTP request;
       * the current implementation does not allow more than
       * one outstanding request at a time
       */
      val isPending = !pending
        .compareAndSet(false, true)
      if (isPending || handler.isComplete) {
        return
      }
      /*
       * STEP #2: Retrieve next request uri and perform
       * the respective HTTP request
       */
      val uri = handler.getNextUri(endpoint)
      /*
       * The HTTP request leverages the `singleGet` method
       * from the pooled HTTP client with exponential backoff
       */
      singleGet(uri, headers, backoff)
        .map(s => callback.invoke(Success(s)))
        .recover {
          case t => callback.invoke(Failure(new Exception(t.getLocalizedMessage)))
        }

    }
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
      case (Success(answer), _) =>
        /*
         * Register HTTP response for further processing;
         * note, this also enables to retrieve the next
         * request URL from the response header
         */
        handler.setHttpResponse(answer)
        /*
         * Proceed with the evaluation of the provided
         * response status code. This implementation
         * supports
         *
         * - 200
         * - 422
         * - 429
         *
         * explicitly, and all other status codes run
         * into a kill switch.
         */
        answer match {
          /*
           * Handling regular Http responses
           */
          case HttpResponse(StatusCodes.OK, _, entity, _) =>
            Future {
              entity.withoutSizeLimit().dataBytes
            }

          case HttpResponse(StatusCodes.UnprocessableEntity, _, entity, _) =>
            Future {
              entity.dataBytes
            }
          /*
           * Handling (429) Too many requests, and apply
           * exponential backoff
           */
          case HttpResponse(StatusCodes.TooManyRequests, _, entity, _) =>
            entity.discardBytes()
            throw TooManyRequests

          case HttpResponse(statusCode, _, entity, _) =>
            entity.discardBytes()
            killSwitch.shutdown()

            throw new Exception(s"Request to Http endpoint returns with: ${statusCode.value}.")
        }
      case (Failure(failure), _) => throw failure

    }

    override def onPull(): Unit = {

       if (handler.isComplete) {
         completeStage()
       }
       else if (buffer.isEmpty) {
         /*
          * Do nothing and wait for the first request
          * from `preStart` to complete
         */
         ()
       }
       else {
         readNextPage()
         if (buffer.nonEmpty) {
           push(out, buffer.head)
           buffer = buffer.tail

         }
       }
    }
  }
}
