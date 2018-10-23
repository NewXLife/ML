package beginners_guide_scala

import org.apache.livy.{Job, JobContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Failure, Success, Try}

class TrySuccessFailureTest() extends Job[Unit]{
  override def call(jc: JobContext): Unit = {
    val spark = jc.sparkSession().asInstanceOf[SparkSession]
      val df:Try[DataFrame] = Try(spark.read.json("hdfs://172.16.37.121:8020/user/test-env/test/data/paraJson"))

    df match {
      case Success(d) => d.createOrReplaceTempView("test")
      case Failure(ex) => println(s"${ex.getMessage}")
    }
  }
}