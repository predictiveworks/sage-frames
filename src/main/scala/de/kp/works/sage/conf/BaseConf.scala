package de.kp.works.sage.conf

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

import com.typesafe.config.{Config, ConfigFactory}

abstract class BaseConf {
  /**
   * The (internal) resource folder file name
   */
  var path: String
  /**
   * The name of the configuration file used
   * with logging
   */
  var logname: String
  /**
   * In case of a deployed `SageFrames` server, the file system
   * path to the configuration folder is provided as system
   * property `config.dir`
   */
  val folder: String = System.getProperty("config.dir")
  /**
   * This is the reference to the overall configuration
   * file that holds all configuration required for this
   * application
   */
  var cfg: Option[Config] = None

  def getCfg(name: String): Config = {

    if (cfg.isEmpty)
      throw new Exception(s"Configuration not initialized.")

    cfg.get.getConfig(name)

  }

  def init(): Boolean = {

    if (cfg.isDefined) true
    else {
      try {

        val config = loadAsString
        cfg = if (config.isDefined) {
          /*
           * An external configuration file is provided
           * and must be transformed into a Config
           */
          Option(ConfigFactory.parseString(config.get))

        } else {
          /*
           * The internal reference file is used to
           * extract the required configurations
           */ Option(ConfigFactory.load(path))

        }
        true

      } catch {
        case _: Throwable =>
          false
      }
    }
  }

  def isInit: Boolean = {
    cfg.isDefined
  }

  def loadAsString: Option[String] = {

    try {

      val configFile =
        if (folder == null) None else Some(s"$folder$path")

      if (configFile.isEmpty) {
        println(s"Launch `SageFrames` with internal $logname configuration.")
        None

      } else {
        println(s"Launch `SageFrames` with external $logname configuration.")

        val source = scala.io.Source.fromFile(new java.io.File(configFile.get))
        val config = source.getLines.mkString("\n")

        source.close
        Some(config)

      }

    } catch {
      case t: Throwable =>
        println(s"Loading `SageFrames` $logname configuration failed: ${t.getLocalizedMessage}")
        None
    }

  }

}
