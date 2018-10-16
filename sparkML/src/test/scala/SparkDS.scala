import java.util.Properties

object SparkDS extends App {

  val spark = SparkSession.builder().appName("test-ds").master("local[*]").getOrCreate()


  //data-source from json ,orc, parquet, text, csv, jdbc
  val jsonDF = spark.read.format("json").load("/Users/colin/Documents/bd_ML/src/main/resources/paraJson")
  jsonDF.show()

  val csv = spark.read.csv("")
  val parquet = spark.read.parquet("")
  val orc = spark.read.orc("")
  val text = spark.read.text("")


  //jdbc
  // Note: JDBC loading and saving can be achieved via either the load/save or jdbc methods
  // Loading data from a JDBC source
  val jdbcDF = spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql:dbserver")
    .option("dbtable", "schema.tablename")
    .option("user", "username")
    .option("password", "password")
    .load()

  val connectionProperties = new Properties()
  connectionProperties.put("user", "username")
  connectionProperties.put("password", "password")
  val jdbcDF2 = spark.read
    .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)

  // Saving data to a JDBC source
  jdbcDF.write
    .format("jdbc")
    .option("url", "jdbc:postgresql:dbserver")
    .option("dbtable", "schema.tablename")
    .option("user", "username")
    .option("password", "password")
    .save()

  jdbcDF2.write
    .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)
}
