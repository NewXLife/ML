package com.niuniuzcd.demo.util

import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.livy.JobContext
import org.apache.livy.scalaapi.{LivyScalaClient, ScalaJobContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * Data source handler
  */
object DSHandler {
  val scalaClient: LivyScalaClient = null

  val pros: Properties = InitEnv.getProperties("dev")
  val url: String = pros.getProperty("mysql.url")
  val username: String = pros.getProperty("mysql.username")
  val password: String = pros.getProperty("mysql.password")
  val driver: String = pros.getProperty("mysql.driver")

  def loadJDBC(context: ScalaJobContext): DataFrame = {
    context.sparkSession[SparkSession].read.format("jdbc")
      .option("url", url)
      .option("user", username)
      .option("password", password)
      .load()
  }

  val connectionProperties = new Properties()
  connectionProperties.put("driver", driver)
  connectionProperties.put("user", username)
  connectionProperties.put("password", password)

  def save2MysqlDb(jdbcDF: DataFrame, dbTable: String, saveMode: String = "append"): Unit = {
    println("last step save2db")
    jdbcDF.coalesce(100).write.mode(saveMode).jdbc(url, dbTable, connectionProperties)
  }


}




