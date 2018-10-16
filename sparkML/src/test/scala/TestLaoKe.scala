import java.util

import scala.collection.JavaConversions._
import com.kuainiu.beidou.domain.data.ds.{DSType, SchemaItem}
import com.kuainiu.beidou.domain.data.ds.store.{FileType, StoreEngine}
import com.kuainiu.beidou.livy.LivyCore
import com.kuainiu.beidou.statistic.{SampleStatistic, StatisticCenter}
import com.kuainiu.beidou.util.{DataFrameUtil, DataUtils, Tools}
import com.kuainiu.demo.PiApp
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

/**
  * create by colin on 2018/8/15
  */
object TestLaoKe extends App{
  val spark = SparkSession.builder().appName("test-ds").master("local[*]")
    .config("spark.executor.momory","2g")
    .getOrCreate()

  import spark.implicits._
  import spark.sql

  spark.sparkContext.setLogLevel("ERROR")
  val df = spark.read.format("csv").option("header", "true").load("E:\\NewX\\newX\\bd-engine\\docs\\query_result.csv").repartition(10)
//  df = spark.read.format("csv").option("header", "true").schema(fieldSchema).load("D:\\NewX\\bd_ML\\docs\\laoke_online.csv").limit(100)
  df.createOrReplaceTempView("test")

  df.show(10,false)

  import  spark.implicits._
  import spark.sql
//  StatisticCenter.apply.String2IntType(df, name)

  val su = new util.ArrayList[SchemaItem]
  val scatterFileds = "apply_risk_id,apply_risk_created_at,overdue_days,max_yuqi_day,min_yuqi_day,last_latest_created_span,first_last_created_span,mean_first_last_created_span,latest_yuqi_day,farest_yuqi_day,danqi_num,duoqi_num,latest_borrow_span,farest_created_span,mean_created_span,baidu_panshi_prea_score,baidu_panshi_duotou_name_score,baidu_panshi_duotou_identity_score,baidu_panshi_duotou_phone_score,baidu_panshi_black_score,baidu_panshi_black_count_level1,baidu_panshi_black_count_level2,baidu_panshi_black_count_level3,fbi_score,fbi_desc,xinyan_score,anti_fraud_old,anti_fraud,anti_fraud_version_two_point_zero,xcloud_score,jd_ss_score_payday_sort_score,td_score_final_score,umeng_credit_score_credit_score,max_call_in_len,avg_call_in_len,sum_call_in_len,std_call_in_len,max_call_cnt,avg_call_cnt,sum_call_cnt,std_call_cnt,max_call_len,avg_call_len,sum_call_len,std_call_len,max_call_out_cnt,avg_call_out_cnt,sum_call_out_cnt,std_call_out_cnt,max_call_out_len,avg_call_out_len,sum_call_out_len,std_call_out_len,max_call_in_cnt,avg_call_in_cnt,sum_call_in_cnt,std_call_in_cnt,max_1w,avg_1w,sum_1w,std_1w,count_1m,max_1m,avg_1m,sum_1m,std_1m,max_3m,avg_3m,sum_3m,std_3m,max_early_morning,avg_early_morning,sum_early_morning,std_early_morning,max_morning,avg_morning,sum_morning,std_morning,max_noon,avg_noon,sum_noon,std_noon,max_afternoon,avg_afternoon,sum_afternoon,std_afternoon,max_night,avg_night,sum_night,std_night,sum_all_day,max_weekday,avg_weekday,sum_weekday,std_weekday,max_weekend,avg_weekend,sum_weekend,std_weekend,max_holiday,avg_holiday,sum_holiday,std_holiday,call_cnt_gre_5,call_cnt_equal_1,call_cnt_gre_5_ratio,call_cnt_equal_1_ratio,call_len_eql_gre_1,call_len_les_1,coc_gre_0,coc_equal_0,coc_gre_0_ratio,cic_gre_0,cic_equal_0,cic_gre_0_ratio,c1w_gre_0,c1w_equal_0,c1w_gre_0_ratio,c1m_gre_0,c1m_equal_0,c1m_gre_0_ratio,c3m_gre_0,c3m_equal_0,c3m_gre_0_ratio,ccem_gre_0,ccem_equal_0,ccem_gre_0_ratio,ccm_gre_0,ccm_equal_0,ccm_gre_0_ratio,ccn_gre_0,ccn_equal_0,ccn_gre_0_ratio,ca_gre_0,ca_equal_0,ca_gre_0_ratio,cn_gre_0,cn_equal_0,cn_gre_0_ratio,ccwd_gre_0,ccwd_equal_0,ccwd_gre_0_ratio,ccwe_gre_0,ccwe_equal_0,ccwe_gre_0_ratio,count_region,max_avg_cit,diffdate,equipment_app_name,equipment_app_name_1,call_len_then_equal_1_ratio,7d,dt"

  for (name <- scatterFileds.split(",")){
    val ts_test = new SchemaItem()
    ts_test.setDesc("test")
    ts_test.setName(name)
    ts_test.setPartition(true)
    ts_test.setType("string")

    su.add(ts_test)
  }



//  for (x <- scatterFileds.split(",")){
//    val temp = SampleStatistic.eachV(df,x)
//    println("origin:",temp)
//    val tt = Tools.filterSpecialChar(temp)
//    println("after:",tt)
//  }
//  val df2 = sql("select apply_risk_id from test")


//  println("------start-----:", DataUtils.getNowDate)
//
//  println("------start-----:", DataUtils.getNowDate)

//  val newDf = df2.withColumn("id",org.apache.spark.sql.functions.row_number().over(Window.partitionBy("apply_risk_id").orderBy("apply_risk_id"))).show(10)



//val newRdd = newDf.rdd.repartition(10)
  //val df = session.createDataFrame(newRdd,schema)


//  val record = tempRdd


//  val df2 = sql("select * from test where biz_report_expect_at")


//  val filterCol = "biz_report_expect_at"
//  val dtest  = df.select(filterCol).limit(1).first().get(0).toString
//  println("######--->", dtest.length)


//StatParametersProtocol genProtocol = genProtocol(StoreEngine.hdfs, DSType.dataset, FileType.csv, "hdfs://nameservicestream/user/model/laoke_online.csv");
//  val pFile =  PiApp.genProtocol(StoreEngine.hdfs, DSType.dataset, FileType.csv, "hdfs://172.16.37.121:9000/user/file/laoke_online.csv")


//  val conList = new util.ArrayList[SchemaItem]
//  val t = new SchemaItem()
//  t.setDesc("test")
//  t.setName("ty2")
//  t.setPartition(true)
//  t.setType("int")
//
//  val t1= new SchemaItem()
//  t1.setDesc("test")
//  t1.setName("ty2forqnn")
//  t1.setPartition(true)
//  t1.setType("int")
//
//  val t3= new SchemaItem()
//  t3.setDesc("test")
//  t3.setName("baidu_panshi_black_match")
//  t3.setPartition(true)
//  t3.setType("int")
//
//
//  val lebel= new SchemaItem()
//  lebel.setDesc("test")
//  lebel.setName("overdue_days")
//  lebel.setPartition(true)
//  lebel.setType("int")
//
//  conList.add(t1)
//  conList.add(t)
//  conList.add(t3)
//
//  val res = conList
//  StatisticCenter.apply.execCon(df, conList.toList, 100, 111, lebel)

//mean_yuqi_day, std_yuqi_day


//
//  spark.stop()

//  val ts0 = new SchemaItem()
//  ts0.setDesc("test")
//  ts0.setName("biz_report_expect_at")
//  ts0.setPartition(true)
//  ts0.setType("int")
//
//  val ts1= new SchemaItem()
//  ts1.setDesc("test")
//  ts1.setName("latest_borrow_span")
//  ts1.setPartition(true)
//  ts1.setType("int")
//
//  val ts2= new SchemaItem()
//  ts2.setDesc("test")
//  ts2.setName("danqi_num")
//  ts2.setPartition(true)
//  ts2.setType("int")
//
//
//  val ts3= new SchemaItem()
//  ts3.setDesc("test")
//  ts3.setName("farest_created_span")
//  ts3.setPartition(true)
//  ts3.setType("int")
//
//  su.add(ts0)
//  su.add(ts1)
//  su.add(ts2)
//  su.add(ts3)



  // var indexId = col.hashCode + dsId.hashCode() * 17 + stId.hashCode() * 13


//  val res = SampleStatistic.quantile(df, "ty2",Array(0, 0.25, 0.5, 0.75, 1))
//
//  res.foreach(println(_))

  //selectExpr("CAST(key AS Double)")
//
//  val newDf = df.selectExpr("CAST(ty2 as Double)")
//  val col = "ty2"
//  println("min-----------------")
//  val min = df.selectExpr("CAST(ty2 as Double)").agg(s"$col" -> "min").first().get(0)
//  println("min========:", min.asInstanceOf[Double])
//
//  newDf.show()

 // if (containNa) df.select(df(s"$col")).count() else df.select(df(s"$col")).filter(df(s"$col") === "" or df(s"$col") === null).count()
//   val t1= df.select(df(s"$col")).count()
//  val t2 = df.where(df(s"$col").isNotNull).count()
//  val t3 = df.where(df(s"$col").isNull).count()
//  val t4 = df.where("ty2 is null").count()
//  val t5 = df.where("ty2 is not null").count()

//  println("##################", t1, t2, t3, t4, t5)

//  newDf.sort(-newDf(s"$col")).select(newDf(s"$col")).groupBy(newDf(s"$col")).count().map(x => (x.get(0), x.get(1).asInstanceOf[Long] / t1)).show()
//  newDf.sort(-newDf(s"$col")).groupBy(newDf(s"$col"))

//  newDf.createOrReplaceTempView("test")
//
//
//  val over = df.select("overdue_days")
//  println("-----test-----------")
//  over.show(100, false
//  )
//  over.groupBy("overdue_days").count().sort($"overdue_days".desc).select($"overdue_days",$"count"/100.0).show(100,false)
//
//
//
//  val res = newDf.groupBy(s"$col").count().sort(-$"$col").select($"$col",$"count"/100.0).toDF("key","value").limit(5)
//
//
//  println("rrrrrrrrrrrrr",res.toDF("key", "value").first().toString)
//
//  res.createOrReplaceTempView("m")
//  val temp = df.sparkSession.sql("select concat_ws(';',collect_set(each)) from (select concat_ws(',',key,value) as each from m)")
//  temp.show(10, false)


//  val arr = Array(0.1,0.2,0.3).mkString(",")
//  println("------",arr)
  /**
    * +-------------------------------------------------+
    * |concat_ws(;, collect_set(each))                  |
    * +-------------------------------------------------+
    * |68.0,0.01;55.0,0.01;38.0,0.01;41.0,0.02;39.0,0.02|
    * +-------------------------------------------------+
    */

//  newDf.sort(-newDf(s"$col")).show()
//  df.select($"ty2".getItem(1).cast("double")).show()

//  val fieldSchema = StructType(Array(
//    StructField("apply_risk_id",StringType,true),
//    StructField("overdue_days",StringType,true),
//    StructField("biz_report_expect_at",StringType,true),
//    StructField("ty2",DoubleType,true),
//    StructField("ty2forqnn",DoubleType,true),
//    StructField("baidu_panshi_black_match",DoubleType,true),
//    StructField("baidu_panshi_black_score",DoubleType,true),
//    StructField("baidu_panshi_black_count_level1",DoubleType,true),
//    StructField("baidu_panshi_black_count_level2",DoubleType,true),
//    StructField("baidu_panshi_black_count_level3",DoubleType,true),
//    StructField("baidu_panshi_duotou_name_match",DoubleType,true),
//    StructField("baidu_panshi_duotou_name_score",DoubleType,true),
//    StructField("baidu_panshi_duotou_name_detail_key",DoubleType,true),
//    StructField("baidu_panshi_duotou_name_detail_val",DoubleType,true),
//    StructField("baidu_panshi_duotou_identity_match",DoubleType,true),
//    StructField("baidu_panshi_duotou_identity_score",DoubleType,true),
//    StructField("baidu_panshi_duotou_identity_detail_key",StringType,true),
//    StructField("baidu_panshi_duotou_identity_detail_val",StringType,true),
//    StructField("baidu_panshi_duotou_phone_match",DoubleType,true),
//    StructField("baidu_panshi_duotou_phone_score",DoubleType,true),
//    StructField("baidu_panshi_duotou_phone_detail_key",StringType,true),
//    StructField("baidu_panshi_duotou_phone_detail_val",DoubleType,true),
//    StructField("baidu_panshi_prea_models",StringType,true),
//    StructField("baidu_panshi_prea_score",StringType,true),
//    StructField("apply_times",StringType,true),
//    StructField("age",StringType,true),
//    StructField("gender",StringType,true),
//    StructField("prov",StringType,true),
//    StructField("city",StringType,true),
//    StructField("max_yuqi_day",StringType,true),
//    StructField("min_yuqi_day",StringType,true),
//    StructField("mean_yuqi_day",StringType,true),
//    StructField("latest_yuqi_day",StringType,true),
//    StructField("farest_yuqi_day",StringType,true),
//    StructField("danqi_num",StringType,true),
//    StructField("duoqi_num",StringType,true),
//    StructField("latest_borrow_span",StringType,true),
//    StructField("farest_created_span",StringType,true),
//    StructField("mean_created_span",StringType,true),
//    StructField("dt",StringType,true)
//    ))
//
//  //类型过滤
//  val ts = df.schema.filter(f => f.dataType != StringType)
//
//  val  connCols =
//    """
//      |ty2,
//      |ty2forqnn,
//      |baidu_panshi_black_match,
//      |baidu_panshi_black_score,
//      |baidu_panshi_black_count_level1,
//      |baidu_panshi_black_count_level2,
//      |baidu_panshi_black_count_level3,
//      |baidu_panshi_duotou_name_match,
//      |baidu_panshi_duotou_name_score,
//      |baidu_panshi_duotou_name_detail_key,
//      |baidu_panshi_duotou_name_detail_val,
//      |baidu_panshi_duotou_identity_match,
//      |baidu_panshi_duotou_identity_score,
//      |baidu_panshi_duotou_phone_match,
//      |baidu_panshi_duotou_phone_score,
//      |baidu_panshi_duotou_phone_detail_val
//      |""".stripMargin
//
//
//  val scatter =
//    """
//      |overdue_days,
//      |biz_report_expect_at,
//      |latest_borrow_span,
//      |farest_created_span,
//      |danqi_num,
//      |duoqi_num,
//      |latest_yuqi_day,
//      |farest_yuqi_day,
//      |max_yuqi_day,
//      |min_yuqi_day,
//      |mean_yuqi_day,
//      |apply_times,
//      |baidu_panshi_prea_models,
//      |baidu_panshi_prea_score
//    """.stripMargin


  //case class ColumnObj(style: String, value: String)
//  StatisticCenter.apply.statistic(df,buffer.toArray, 1, 1, label = "overdue_days")



  //val taxiDF = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").schema(fieldSchema).load("/home/shiyanlou/taxi.csv")
//  spark.read.option("header", value = true).option("multiLine", value = true).csv("file:///home/hadoop/test.csv")


//  val f = Array(
//    "apply_risk_id",
//    "overdue_days",
//    "biz_report_expect_at",
//    "ty2",
//    "ty2forqnn",
//    "baidu_panshi_black_match",
//    "baidu_panshi_black_score",
//    "baidu_panshi_black_count_level1",
//    "baidu_panshi_black_count_level2",
//    "baidu_panshi_black_count_level3",
//    "baidu_panshi_duotou_name_match",
//    "baidu_panshi_duotou_name_score",
//    "baidu_panshi_duotou_name_detail_key",
//    "baidu_panshi_duotou_name_detail_val",
//    "baidu_panshi_duotou_identity_match",
//    "baidu_panshi_duotou_identity_score",
//    "baidu_panshi_duotou_identity_detail_key",
//    "baidu_panshi_duotou_identity_detail_val",
//    "baidu_panshi_duotou_phone_match",
//    "baidu_panshi_duotou_phone_score",
//    "baidu_panshi_duotou_phone_detail_key",
//    "baidu_panshi_duotou_phone_detail_val",
//    "baidu_panshi_prea_models",
//    "baidu_panshi_prea_score",
//    "apply_times",
//    "age",
//    "gender",
//    "prov",
//    "city",
//    "max_yuqi_day",
//    "min_yuqi_day",
//    "mean_yuqi_day",
//    "latest_yuqi_day",
//    "farest_yuqi_day",
//    "danqi_num",
//    "duoqi_num",
//    "latest_borrow_span",
//    "farest_created_span",
//    "mean_created_span",
//    "dt"
//  )




}
