package de.kp.works.sage.api

import org.openqa.selenium.chrome.{ChromeDriver, ChromeOptions}

/*
 * No not forget to install the Chromedriver:
 *
 * brew install chromedriver
 */
object SageChrome {
/*
    /*
    Operate with a headless browser

    https://medium.com/hackernoon/introduction-to-headless-chrome-with-java-b591bc4764a0
     */

 */
  private val driverPath = "/usr/local/Caskroom/chromedriver/100.0.4896.60/chromedriver"
  System.setProperty("webdriver.chrome.driver", driverPath)

  val chromeDriver: ChromeDriver = buildDriver()

  private def buildDriver():ChromeDriver = {

    val chromeOptions = new ChromeOptions()
    chromeOptions.addArguments(
      "--headless", "--disable-gpu", "--window-size=1920,1200","--ignore-certificate-errors", "--silent")
/*
public static void createAndStartService() throws IOException {

      service = new ChromeDriverService.Builder()

              .usingDriverExecutable(new File("/path/to/chromedriver"))

              .usingAnyFreePort()

              .build();

      service.start();

      driver = new RemoteWebDriver(service.getUrl(), new ChromeOptions());
  }
 */
    new ChromeDriver(chromeOptions)

  }

}
