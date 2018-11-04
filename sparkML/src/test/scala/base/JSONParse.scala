package base

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.DataFrame
import util.SparkTools

import scala.collection.immutable.TreeMap
case class P(st: String, ds: String, out: String)
trait Protocol[T]{
  def getDs(s: T): T
  def getSt(s:T) : T
  def getOut(s: T): T

  def parseTrainJson(jsonStr: String): (String, String, String) = {
    val obj = JSON.parseObject(jsonStr, classOf[P])
    (obj.st, obj.ds, obj.out)
  }
}

class FeaSampleSplit(str: String) extends Protocol[String]{
  val (st, ds, out) = parseTrainJson(str)
  def datasetSplit(df: DataFrame):(DataFrame, DataFrame) = {
    val res = df.randomSplit(Array(0.8), seed = 1)
    (res(0), res(1))
  }

  def getDs(s: String): String = {
    val json = s
    s.contains("prevId")
    ""
  }

  def getSt(s: String): String = {
    val json = JSON.parseObject(s)
    val method = json.getString("method")
    val test_size = json.getString("test_size")
    val random_state = json.getString("random_state")
    val time_col = json.getString("time_col")
    val index_col = json.getString("index_col")
    val label_col = json.getString("label_col")
      ""
  }

  def getOut(s: String): String = {
    val json = JSON.parseObject(out)
    val dst = json.getString("dst")
    dst
  }
}

class FeatureTransform(str: String) extends Protocol[String]{
  val (st, ds, out) = parseTrainJson(str)

  case class Encoder(method: String, params:String)
  case class Cate(cols:Array[String], encoders: Array[Encoder])
  case class Cont(cols:Array[String], encoders: Array[Encoder])
  case class Custom(cols:Array[String], encoders: Array[Encoder])
  case class ST(cate: Cate, cont: Cont, custom: Custom, method: String, params: String)

  val  featureTransform = (input: DataFrame) => input

  def getDs(s: String): String = {
    ""
  }

  def getSt(s: String): String = {
    val st = JSON.parseObject(s, classOf[ST])
    val cate = st.cate
    val cont = st.cont
    val custom = st.custom
    val method = st.method
    val params = st.params
    ""
  }

  def getOut(s: String): String = {
    ""
  }
}

class TrainFilter(str: String) extends Protocol[String]{
  val (st, ds, out) = parseTrainJson(str)

  val trainFilter= (df: DataFrame) =>
    df

  def getDs(s: String): String = {
    ""
  }

  def getSt(s: String): String = {
    val json = JSON.parseObject(s)
    val method = json.getString("method")
    val params = json.getString("params")
    val threshold = json.getString("threshold")

    val estimator = JSON.parseObject(params)
    val estimatorName = estimator.getString("estimator")
    val estimatorParameters = estimator.getString("params")
    ""
  }

  def getOut(s: String): String = {
    val json = JSON.parseObject(s)
    val dst = json.getString("dst")
    ""
  }
}

class TrainTrainer(str: String) extends Protocol[String]{
  val (st, ds, out) = parseTrainJson(str)

  def trainFilter(df: DataFrame): DataFrame = {
    null
  }

  def getDs(s: String): String = {
    val json = JSON.parseObject(s)
    ""
  }

  def getSt(s: String): String = {
    case class XGBClassifierP(colsample_bytree: Double,
                              reg_lambda: Long,
                              silent: Boolean,
                             base_score: Double,
                              scale_pos_weight:Int,
                              eval_metric: String,
                              max_depth: Int,
                              n_jobs: Int,
                              early_stopping_rounds: Int,
                              n_estimators:Long,
                              random_state:Int,
                              reg_alpha:Int,
                              booster:String,
                              objective: String,
                              verbose: Boolean,
                              colsample_bylevel: Double,
                              subsample: Double,
                              learning_rate: Double,
                              gamma: Double,
                              max_delta_step: Int,
                              min_child_weight:Int
    )
    val json = JSON.parseObject("s")
    val method = json.getString("method")
    val test_size = json.getString("test_size")
    val oversample = json.getString("oversample")
    val n_folds = json.getString("n_folds")
    val random_state = json.getString("random_state")
    val verbose = json.getString("verbose")

    val params = json.getString("params")
    val xgbcObj = JSON.parseObject(params, classOf[XGBClassifierP])

    ""
  }

  def getOut(s: String): String = {
    val json = JSON.parseObject(s)
    json.getString("dst")
    ""
  }
}

class FeaDs(str: String) extends Protocol[String]{
  val (st, ds, out) = parseTrainJson(str)

  def getDataSet():DataFrame = {
    null
  }

  def getDs(s: String): String = {
    val json = JSON.parseObject(ds)
    val ss = json.get("ss")
    println(ss)
    //return hdfs path or hive-table
    "model.laoke"
  }

  def getSt(s: String): String = {
    val json = JSON.parseObject(st)
    val para = JSON.parseObject(json.getString("params"))
    val sqlStr = StringBuilder.newBuilder
    sqlStr ++= "select " + para.getString("include")
    sqlStr ++= "where " + para.getString("condition")
    sqlStr.toString()
  }

  def getOut(s: String): String = {
    val json = JSON.parseObject(out)
    val dst = json.getString("dst")
    dst
  }
}

object JSONParse extends SparkTools {
  //step1, parse protocol
  val protocolList = "FEA_DS, FEA_SAMPLE_SPLIT, FEA_TRANSFORM, TRAIN_FILTER, TRAIN_TRAINER"
  val fea_ds =
    """{
      |	"st":{
      |		"params":{
      |			"include":"apply_risk_id,apply_risk_created_at,jxl_contact_call_len_then_equal_1_ratio,jxl_contact_c1w_then_0_ratio,farest_created_span_self_stb,jxl_c1m_gre_0_ratio,jxl_c3m_gre_0_ratio,jxl_contact_call_cnt_equal_1_ratio,jxl_ccm_gre_0_ratio,jxl_region_count_region,jxl_phone_diffdate,hit_type_lijihuankuan_d30,baidu_panshi_prea_score,jxl_coc_gre_0_ratio,latest_yuqi_day_self_stb,mean_first_last_created_span_self_stb,last_latest_created_span_self_stb,hit_type_bangdingshoukuanyinhangka_d30,kuaidi_cnt,max_yuqi_day_self_stb,yiyuqi_cnt,baidu_panshi_duotou_identity_score,jxl_avg_morning,first_last_created_span_self_stb,enter_type_querenjiekuan_d30,fbi_score,overdue_days,equipment_app_names_v2 as equipment_app_name,i011,i061,i301,i601",
      |			"condition":"product_period_count=1 and apply_risk_created_at>= '2018-07-20'and apply_risk_created_at <= '2018-08-05'",
      |			"min_date":"from_unixtime(unix_timestamp(date_sub(current_date(),120),'yyyy-MM-dd'),'yyyyMMdd')",
      |			"max_date":"from_unixtime(unix_timestamp(date_sub(current_date(),0),'yyyy-MM-dd'),'yyyyMMdd')"
      |		}
      |	},
      |	"ds":{
      |		"dsType":"samplesrc",
      |		"fileType":"hive",
      |		"id":137,
      |		"label":{
      |			"desc":"label",
      |			"name":"7d",
      |			"type":"number"
      |		}
      |	},
      |	"out":{
      |		"dst":"/model/${flowId}/${nodeId}/ds_out.csv"
      |	}
      |}""".stripMargin
  val sample_split =
    """
      |{
      |    "st":{
      |        "method":"oot",
      |        "test_size":0.2,
      |        "random_state":7,
      |        "time_col":"apply_risk_created_at",
      |        "index_col":"apply_risk_id",
      |        "label_col":"overdue_days",
      |        "py_split":"true"
      |    },
      |    "ds":"#state(${prevId},ds)",
      |    "out":{
      |        "dst":"/model/${flowId}/${nodeId}"
      |    }
      |}
    """.stripMargin

  var trainMap = TreeMap.empty[String, Protocol[Any]]

  //fea_ds
  val ds = new FeaDs(fea_ds).getDataSet()

  //get train,test dataframe
  val (train, test) = new FeaSampleSplit(sample_split).datasetSplit(ds)

  val t_train = new FeatureTransform("").featureTransform(train)
  val t_test = new FeatureTransform("").featureTransform(test)

  val train_filter = new TrainFilter("").trainFilter(t_train)

  val model = new TrainTrainer("").trainFilter(train_filter)

  //input t_test production result

  //defined a pipeline function
  val pipeline = Function.chain(Seq(new FeatureTransform("").featureTransform, new TrainFilter("").trainFilter))

  //execute pipeline
  pipeline(train)

  //we suppose the sequence is protocolList


  //step2, defined a train flow，build a pipeline flow
}
