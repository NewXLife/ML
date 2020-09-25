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
//  val scalaClient: LivyScalaClient = null

//  val pros: Properties = InitEnv.getProperties("dev")
//  val url: String = pros.getProperty("mysql.url")
//  val username: String = pros.getProperty("mysql.username")
//  val password: String = pros.getProperty("mysql.password")
//  val driver: String = pros.getProperty("mysql.driver")

//  def loadJDBC(context: ScalaJobContext): DataFrame = {
//    context.sparkSession[SparkSession].read.format("jdbc")
//      .option("url", url)
//      .option("user", username)
//      .option("password", password)
//      .load()
//  }
//
//  val connectionProperties = new Properties()
//  connectionProperties.put("driver", driver)
//  connectionProperties.put("user", username)
//  connectionProperties.put("password", password)

//  def save2MysqlDb(jdbcDF: DataFrame, dbTable: String, saveMode: String = "append"): Unit = {
//    jdbcDF.coalesce(100).write.mode(saveMode).jdbc(url, dbTable, connectionProperties)
//  }

//  def read2MysqlDb(spark: SparkSession, tb: String):DataFrame = {
//    spark.read.jdbc(url, tb, connectionProperties)
//  }

  /**
    * spark.read.format("jdbc")
    * .option("url", jdbcUrl)
    * .option("query", "select c1, c2 from t1")
    * .load()
    *
    * Dataset<Row> ds = sparkSession.read()
    * .format("jdbc")
    * .option("url", "jdbc:hive2://10.18.100.116:10000/zgl")
    * .option("user", "cyzx")
    * .option("password", "wwcy@123.com")
    * .option("driver", "org.apache.hive.jdbc.HiveDriver")
    * .option("query", "select * from user_info")
    * .option("dbtable","user_info").load();
    */
  def readJdbc(spark: SparkSession): DataFrame = {
    spark.read
      .format("jdbc")
      .option("url", "jdbc:hive2://10.18.100.116:10000/gda_test")
//      .option("dbtable", tb)password":"gsww@20!8","url":"jdbc:hive2://10.18.100.116:10000/gda_test","username":"dgms
      .option("dbtable", "cart_output_test")
      .option("user", "cyzx")
      .option("password", "wwcy@123.com")
      .load()
  }

}




