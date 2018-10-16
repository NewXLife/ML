package com.niuniuzcd.demo.util

import java.util.Properties

object InitEnv {
  def getProperties(env: String): Properties = {
    println(s"get env parameter:$env")
    val properties = new Properties()
    //    val path = Thread.currentThread().getContextClassLoader.getResource("cfg-"+ env + ".properties").getPath
    //    properties.load(new FileInputStream(path))
    try {
      val inputFile = this.getClass.getClassLoader.getResourceAsStream("conf-" + env + ".properties")
      properties.load(inputFile)
    } catch {
      case ex: NullPointerException => {
        println(s"load cfg file error:$ex")
      }
    }
    properties
  }
}
