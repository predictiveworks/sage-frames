package de.kp.works.sage.spark

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
import ch.qos.logback.classic.Level
import de.kp.works.sage.conf.SageConf
import de.kp.works.sage.logging.Logging
import org.apache.spark.sql._
import org.slf4j.LoggerFactory

object Session extends Logging {

  /**
   * The internal configuration is used, if the current
   * configuration is not set here
   */
  if (!SageConf.isInit) SageConf.init()
  private val hadoopCfg = SageConf.getHadoopCfg

  System.setProperty("hadoop.home.dir", hadoopCfg.getString("folder"))
  private var session:Option[SparkSession] = None
  /**
   * The SparkSession is built without using the respective
   * builder as this approach can be used e.g. to integrate
   * Analytics-Zoo additional context with ease.
   */
  def initialize():Unit = {
    /*
     * Set log levels for Apache Spark console output programmatically.
     * Steps taken prior to the following ones
     *
     * - Exclude log4j support from Apache Spark (see pom.xml)
     * - Redirect log4j to slf4j
     * - Set logback
     */
    val entries = Seq(
      "io.netty",
      "org.apache.hadoop",
      "org.apache.spark",
      "org.spark_project")

    entries.foreach(entry => {
      val logger = LoggerFactory
        .getLogger(entry).asInstanceOf[ch.qos.logback.classic.Logger]

      logger.setLevel(Level.WARN)

    })

    val spark = SparkSession
      .builder()
      .appName("SageFrames")
      .master("local[*]")
      /*
       * The Spark Web UI leverages Netty and the
       * respective version is in conflict with
       * Selenium
       */
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    session = Some(spark)

  }

  def getSession: SparkSession = {
    if (session.isEmpty) initialize()
    session.get

  }

}