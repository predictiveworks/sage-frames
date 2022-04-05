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
import de.kp.works.sage.conf.SageConf
import org.apache.spark.sql._

object Session {

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