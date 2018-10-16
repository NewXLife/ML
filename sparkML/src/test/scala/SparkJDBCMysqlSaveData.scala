import java.util.Properties

import org.apache.spark.sql.SparkSession


/**
  * 使用jdbc的方式 需要將驅動包提前放到lib庫上
  * 或者使用spark-submit 提交的方式  跟上參數 --jars /opt/lib2/mysql-connector-java-5.1.26-bin.jar
  * 或者在${SPARK_HOME}/conf目录下的spark-defaults.conf中添加：spark.jars /opt/lib/mysql-connector-java-5.1.26-bin.jar
  * 流式使用kafka 處理的時候，注意kafka版本
  */
object SparkJDBCMysqlSaveData extends App {
  val spark = SparkSession.builder().master("local[*]").appName("testJDBC").getOrCreate()
  // Note: JDBC loading and saving can be achieved via either the load/save or jdbc methods
  // Loading data from a JDBC source


  val connectionProperties = new Properties()
  connectionProperties.put("user", "root")
  connectionProperties.put("password", "123")

  val jdbcDF2 = spark.createDataFrame(Seq((3,"2018-05-13","2018-05-13",0,"2018-05-13"))).toDF("sta_id", "sta_start","sta_end","sta_status","update_time")
  jdbcDF2.show()

  val url = "jdbc:mysql://172.16.38.128:3306/beidou?useUnicode=true&amp;characterEncoding=utf-8"
  val db_table = "beidou.statistic_list"
  jdbcDF2.show()

  jdbcDF2.createOrReplaceTempView("test")

//  spark.sql("insert into beidou.statistic_list  values(3,\"2018-05-13\",\"2018-05-13\",0,\"2018-05-13\")")

  jdbcDF2.write.mode("append")
    .jdbc(url, db_table, connectionProperties)

  // Saving data to a JDBC source
//  df.write
//    .format("jdbc")
//    .option("url", "jdbc:postgresql:dbserver")
//    .option("dbtable", "schema.tablename")
//    .option("user", "username")
//    .option("password", "password")
//    .save()


  //  // Specifying the custom data types of the read schema
  //  connectionProperties.put("customSchema", "id DECIMAL(38, 0), name STRING")
  //  val jdbcDF3 = spark.read
  //    .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)


//
//  val jdbcDF2 = spark.read
//    .jdbc("jdbc:mysql://172.16.38.128:3306/beidou?useUnicode=true&amp;characterEncoding=utf-8", "beidou.QRTZ_TRIGGERS", connectionProperties)

  // Specifying create table column data types on write

  //    val jdbcDF = spark.read
  //      .format("jdbc")
  //      .option("url", "jdbc:postgresql:dbserver")
  //      .option("dbtable", "schema.tablename")
  //      .option("user", "username")
  //      .option("password", "password")
  //      .load()
  //


  //  jdbcDF.write
  //    .option("createTableColumnTypes", "name CHAR(64), comments VARCHAR(1024)")
  //    .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)
}
