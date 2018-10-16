

case class JO(ds: String, ut: String, ot: Se)

case class Se(ds: String, ut: String)

case class St[T](st: T)

//case class All(ds:String, st:String, out: St[JO])

case class Out(dst: String)

case class DTime(name: String, ttype: String)

case class Label(name: String, ltype: String)

case class FeaDs(id: Int, dsType: String, label: Label, table: String, time: DTime)

object JsonT extends App {

  //  val jsonStr = "{\n  \"ds\":{\n    \"dsType\":\"csv\",\n    \"label\":{\n      \"name\":\"overdue\",\n      \"type\":\"number\"\n    },\n    \"table\":\"/Users/joe/dataTest/sampling/ds/data.csv\",\n    \"time\":{\n      \"name\":\"apply_risk_created_at\",\n      \"type\":\"date\"\n    }\n  },\n  \"st\":{\n\n  },\n  \"out\":{\n    \"dst\":\"/Users/joe/dataTest/${flowId}/${nodeId}/ds_out.csv\"\n  }\n}"
  //  val te = "{ \"ds\":\"tiger\" }"
  //  val te2 = "{ \"ds\":\"tiger\",\"ut\":\"one\" ,\"ot\":{\"ds\":\"tiger\",\"ut\":\"one\"}}"
  //
  //  type S  = Se
  //  val jsonS = JSON.parseObject(te2, classOf[JO])
  //
  //  println(jsonS.ot.ds)


  case class FrameExample[D, S, O](ds: D, st: S, out: O)

  val jsonstr = "{\n  \"ds\": {\n    \"id\": 19,\n    \"dsType\": \"csv\",\n    \"label\": {\n      \"name\": \"overdue_days\",\n      \"type\": \"number\"\n    },\n    \"table\": \"/model/ds/all_50_15.csv\",\n    \"time\": {\n      \"name\": \"biz_report_expect_at\",\n      \"type\": \"date\"\n    }\n  },\n  \"st\": {},\n  \"out\": {\n    \"dst\": \"/model/${flowId}/${nodeId}/ds_out.csv\"\n  }\n}"


  //  case  class FrameExample1 (ds:FeaDs,st: St[Any] ,out: Out)
  FlowEnum.withName("FEA_DS") match {
    case FlowEnum.FEA_DS =>

      val jsonS = JSON.parseObject(jsonstr, classOf[FrameExample[Any, Any, Any]])

      val sed = JSON.parseObject(jsonS.out.toString, classOf[Out])
      println(jsonS.out)
      println(sed.dst)

      val sed2 = JSON.parseObject(jsonS.out.toString)

      println(sed2.keySet().getClass.getSimpleName)

      sed2.keySet().asScala.foreach {
        case "dst" => println("dst")
        case _ => println("mismatch")
      }


      if (sed2.containsKey("dst")) {
        println(sed2.getString("dst"))
      }

    case FlowEnum.FEA_SAMPLING => println("sampling")
    case FlowEnum.FEA_SAMPLE_SPLIT => println("sample-split")
    case FlowEnum.FEA_TRANSFORM => println("transform")
    case FlowEnum.TRAIN_FILTER => println("train-filter")
    case FlowEnum.TRAIN_TRAINER => println("train")
    case _ => println("mismatch")
  }


  val follow = FlowEnum.FEA_DS :: FlowEnum.FEA_SAMPLING :: FlowEnum.FEA_SAMPLING :: FlowEnum.FEA_SAMPLE_SPLIT :: FlowEnum.FEA_TRANSFORM :: FlowEnum.TRAIN_FILTER :: FlowEnum.TRAIN_TRAINER :: Nil

  println(follow)
}
