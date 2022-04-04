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

import org.openqa.selenium.chrome.{ChromeDriver, ChromeOptions}

/*
 * No not forget to install the Chromedriver:
 *
 * brew install chromedriver
 */
object SageChrome {

  private val driverPath = "/usr/local/Caskroom/chromedriver/100.0.4896.60/chromedriver"
  System.setProperty("webdriver.chrome.driver", driverPath)

  val chromeDriver: ChromeDriver = buildDriver()

  private def buildDriver():ChromeDriver = {

    val chromeOptions = new ChromeOptions()
    chromeOptions.addArguments(
      "--headless", "--disable-gpu", "--window-size=1920,1200","--ignore-certificate-errors", "--silent")

    new ChromeDriver(chromeOptions)

  }

}
