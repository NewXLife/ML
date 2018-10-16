import org.apache.livy.{Job, JobContext}
import org.apache.spark.sql.SparkSession

class Example() extends Job[Unit]{


  override def call(jc: JobContext): Unit = {
    try{
      val spark = jc.sparkSession().asInstanceOf[SparkSession]
      val df = spark.read.json("hdfs://172.16.37.121:8020/user/test-env/test/data/paraJson")
      df.printSchema()
      df.createGlobalTempView("test")

      val df2 = spark.sql("select * from test")

      df2.count()

      println("complete....")
    }catch {
      case e: Exception =>
        e.printStackTrace()
    }

  }
}