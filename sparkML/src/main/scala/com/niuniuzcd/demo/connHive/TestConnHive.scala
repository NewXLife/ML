package com.niuniuzcd.demo.connHive

import org.apache.spark.sql.SparkSession
import java.io.File

import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}

//case object HiveSqlDialect extends JdbcDialect {
//  override def canHandle(url: String): Boolean = url.startsWith("jdbc:hive2")
//
//  override def quoteIdentifier(colName: String): String = {
//    colName.split('.').map(part => s"`$part`").mkString(".")
//  }
//}
//
//class RegisterHiveSqlDialect {
//  def register(): Unit = {
//    JdbcDialects.registerDialect(HiveSqlDialect)
//  }
//}


/**
  * @author zcd
  * @date 2019-09-10 15:00
  */
object TestConnHive extends App {

  // warehouseLocation points to the default location for managed databases and tables
  val warehouseLocation = new File("spark-warehouse").getAbsolutePath
  val  remoteHourse = "hdfs://localhost:8020/user/hive/warehouse"
//  new RegisterHiveSqlDialect().register();
  val spark = SparkSession
    .builder()
    .appName("Spark Hive Example")
    .master("local")
//    .config("hive.metastore.uris", "thrift://10.18.100.116:9083")//指定hive的metastore的地址
//    .config("spark.sql.warehouse.dir", remoteHourse)
    .enableHiveSupport()
    .getOrCreate()



  import spark.implicits._
  import spark.sql
  sql("show  databases").show()
//  sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
//  sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src")

  // Queries are expressed in HiveQL
//  sql("SELECT * FROM src").show()

//  val url = ""
  val jdbcDF = spark.read
    .format("jdbc")
    .option("url", "jdbc:hive2://localhost:10000/dw")
    .option("user", "hive")
    .option("password", "hive")
    .option("driver", "org.apache.hive.jdbc.HiveDriver")
    .option("dbtable","ml_test")
    .option("fetchsize", "100")
    .load();
  //// String huser = cbcEncrypt("dgms" ,"$#0(@9_ec$5ald$&", "$#0(@9_ec$5ald$&");
  //// String hpwd = cbcEncrypt("gsww@20!8" ,"$#0(@9_ec$5ald$&", "$#0(@9_ec$5ald$&");
//
  jdbcDF.show()

//  sql("select * from gda_test.ml_test").show()
}
