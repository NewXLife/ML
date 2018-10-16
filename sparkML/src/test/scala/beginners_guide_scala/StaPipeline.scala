package beginners_guide_scala

import com.kuainiu.beidou.statistic.{SampleStatistic, StatisticCenter}
import org.apache.spark.sql.DataFrame
import util.SparkTools


object StaPipeline extends SparkTools{
  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")
  val base = loadCSVData("csv","E:\\NewX\\newX\\bd-engine\\docs\\20180814_old_tdbase.csv")

  object StaServer{
    abstract class StaIndex(name:String){
      type Value
    }
  }

  import StaServer.StaIndex
  class CommonStaServers{
    var staMap = Map.empty[StaIndex, Any]
    def get(staIndex: StaIndex): Option[staIndex.Value] =
      staMap.get(staIndex).asInstanceOf[Option[staIndex.Value]]

    def set(staIndex: StaIndex)(value: staIndex.Value): Map[StaIndex, Any] ={
      val resMap = staMap.updated(staIndex, value)
      staMap = staMap ++ resMap
      resMap
    }
  }

  type ConnF = (DataFrame, String, Array[String], Long, Long) => String
  type ScatterF = (DataFrame, String, Array[String], Long, Long) => String

  trait SampleIndex extends StaIndex{
    type Value = DataFrame => DataFrame
  }

  trait ConIndex extends StaIndex{
    type Value = ConnF
  }

  trait ScatterIndex extends StaIndex{
    type Value = ScatterF
  }

  object StaIndexs{
    val sampleIndex = new StaIndex("sampleIndex") with SampleIndex
    val conIndex = new StaIndex("conIndex") with ConIndex
    val scatterIndex = new StaIndex("scatterIndex") with ScatterIndex
  }

  val currentStaIndex = new CommonStaServers
  val computerFields = "sampleIndex,conIndex,ksIndex,psiIndex"

  computerFields.split(",").foreach{
    computerField => computerField.trim() match {
      case "conIndex" =>  currentStaIndex.set(StaIndexs.conIndex)(StatisticCenter.execCon)
      case _ => println("""not mashing""")
    }
  }

  def test (staIndex: StaIndex) ={
    currentStaIndex.staMap.getOrElse(staIndex, None)  match {
      case x : ConnF =>
        println("start executing continue index statistics")
        x(base, "apply_date", Array("7day"), 101, 1001)
      case _ =>
        "do nothing"
    }
  }

  test(StaIndexs.conIndex)
//  test(StaIndexs.scatterIndex)

  SampleStatistic.innerMode(base, "7day").show(100, truncate = false)
  val col = "7day"
  import spark.implicits._
//  base.groupBy(s"$col").count().sort($"$col".desc).select($"$col", $"count").show(10, truncate = false)
//  val res1 = base.groupBy(s"$col", "apply_date").count().select($"$col", $"apply_date", $"count").sort($"count".desc).dropDuplicates(Array("apply_date")).sort($"count".desc).createOrReplaceTempView("res1")
//
//  val res2 = base.groupBy("apply_date").count().sort($"count".desc).select($"apply_date", $"count").createOrReplaceTempView("res2")
//
//  val res3 = spark.sql("select res1.7day, res1.apply_date, res1.count/res2.count as mode from res1 left join res2 on res1.apply_date=res2.apply_date")
//  res3.show(10, truncate = false)

//  SampleStatistic.innerModeGrouapByDate(base, "7day", "apply_date").show(10, truncate = false)

  /**
    * +----+----------+-----+
    * |7day|apply_date|count|
    * +----+----------+-----+
    * |-1.0|2018-06-25|2275 |
    * |-1.0|2018-06-24|2015 |
    * |-1.0|2018-06-28|1861 |
    * |-1.0|2018-06-26|1807 |
    * |2.0 |2018-06-24|1412 |
    * |2.0 |2018-06-25|1372 |
    * |-1.0|2018-06-29|1305 |
    * |2.0 |2018-06-28|1242 |
    * |2.0 |2018-06-26|1114 |
    * |3.0 |2018-06-25|953  |
    * +----+----------+-----+
    */
}
