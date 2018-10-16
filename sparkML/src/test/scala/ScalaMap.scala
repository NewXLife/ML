import java.util

import com.kuainiu.beidou.util.FlowEnum

import scala.collection.{SortedMap, mutable}


/**
  * create by colin on 2018/7/13
  */
object ScalaMap extends App {
  val FEA_DS1 = "{\n  \"ds\": {\n    \"id\": 19,\n    \"dsType\": \"csv\",\n    \"label\": {\n      \"name\": \"overdue_days\",\n      \"type\": \"number\"\n    },\n    \"table\": \"/model/ds/all_50_15.csv\",\n    \"time\": {\n      \"name\": \"biz_report_expect_at\",\n      \"type\": \"date\"\n    }\n  },\n  \"st\": {},\n  \"out\": {\n    \"dst\": \"/model/${flowId}/${nodeId}/ds_out.csv\"\n  }\n}"
  val FEA_SAMPLING = "FEA_SAMPLING"
  val FEA_SAMPLE_SPLIT = "FEA_SAMPLE_SPLIT"

  val FEA_TRANSFORM = "FEA_TRANSFORM"

  val TRAIN_FILTER = "TRAIN_FILTER"
  val TRAIN_TRAINER = "TRAIN_TRAINER"

  val parasMap: mutable.LinkedHashMap[String, String] = mutable.LinkedHashMap(
    "FEA_DS" -> FEA_DS1,
    "FEA_SAMPLING" -> FEA_SAMPLING,
    "FEA_SAMPLE_SPLIT" -> FEA_SAMPLE_SPLIT,
    "FEA_TRANSFORM" -> FEA_TRANSFORM,
    "TRAIN_FILTER" -> TRAIN_FILTER,
    "TRAIN_TRAINER" -> TRAIN_TRAINER
  )


  for ((k, v) <- parasMap) {
    FlowEnum.withName(k) match {
      case FlowEnum.FEA_DS => println(v)
      case FlowEnum.FEA_SAMPLING => println(v)
      case FlowEnum.FEA_SAMPLE_SPLIT => println(v)
      case FlowEnum.FEA_TRANSFORM => println(v)
      case FlowEnum.TRAIN_FILTER => println(v)
      case FlowEnum.TRAIN_TRAINER => println(v)
      case _ => println("mismatch")
    }


  }
}
