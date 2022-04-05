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

import com.typesafe.config.Config

object SageConf extends BaseConf {

  override var path = "reference.conf"
  override var logname = "Main"

  def getActorCfg: Config = getCfg("actor")

  def getBindingCfg: Config = getCfg("binding")

  def getHadoopCfg: Config = getCfg("hadoop")

  def getLoggingCfg: Config = getCfg("logging")

  def getSageCfg: Config = getCfg("sage")

}
