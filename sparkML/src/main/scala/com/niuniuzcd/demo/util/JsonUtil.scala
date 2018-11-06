package com.niuniuzcd.demo.util

import java.util

import com.alibaba.fastjson.JSON
import com.google.gson.{JsonObject, JsonParser}

object JsonUtil extends App{


  def gson(str: String): JsonObject = {
    val json = new JsonParser()
    val obj = json.parse(str).asInstanceOf[JsonObject]
    obj
  }

  def getKeySet(str: String): util.Set[String] = {
    gson(str).keySet()
  }

  def json2List(jsonStr: String): Unit = {
    val json = JSON.parseObject(jsonStr)
    println(json.get("et"))
  }

}
