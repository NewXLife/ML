package ml.train

import com.alibaba.fastjson.JSON
import com.niuniuzcd.demo.ml.trainAtom.TrainUnits._
import com.niuniuzcd.demo.util.JsonUtil
import org.apache.spark.sql.DataFrame
import util.SparkTools

import scala.collection.mutable.ArrayBuffer

object TestTrainJob extends SparkTools {


  val protocolStr = TestPro.getJsonSr
  println("get train-job protocol-string: ", protocolStr)


  //protocolstr come from dag-build json-string
  val jsonP = JSON.parseObject(protocolStr)

  //from fea_ds get DataFrame  labelName is "label"
  //  val df = ProtocolHandler.parseTrainProtocol(jobContext, jsonP.getString("FEA_DS")).get

  //the first computer unit must be a resource,the others compose pipeline
  val keySet = JsonUtil.getKeySet(protocolStr)
  println("keyset----------:", keySet)

  val sequence = keySet.toList.drop(1)
  println("executor sequence---------:", sequence)

  //  jsonP.getString("FEA_DS")
  //  jsonP.getString("FEA_SAMPLE_SPLIT")
  //  jsonP.getString("FEA_TRANSFORM")
  //keySet like as : FEA_DS, FEA_SAMPLE_SPLIT, FEA_TRANSFORM, TRAIN_FILTER, TRAIN_TRAINER
  //build pipeline
  type func = DataFrame => DataFrame
  val executorFlow = ArrayBuffer[func]()
  val featureFlow = ArrayBuffer[func]()

  val store = new Components

  //List(FEA_SAMPLE_SPLIT, FEA_TRANSFORM, TRAIN_FILTER, TRAIN_TRAINER))
  for (key <- sequence) {
    println("input keys: ", key)
    //FEA_SAMPLE_SPLIT
    key match {
      case "FEA_SAMPLE_SPLIT" =>
        val so = spliterObj(jsonP.getString("FEA_SAMPLE_SPLIT"))
        store.set(CC.spliter)(so)
        featureFlow += so.spliter
      case "FEA_TRANSFORM" =>
        val fto = featureTransformObj(jsonP.getString("FEA_TRANSFORM"))
        store.set(CC.featureTransform)(fto)
        featureFlow += fto.transform
      case "TRAIN_FILTER" =>
        val to = filterObj(jsonP.getString("TRAIN_FILTER"))
        store.set(CC.filter)(to)
        featureFlow += to.filter
      case "TRAIN_TRAINER" =>
        val to = trainerObj(jsonP.getString("TRAIN_TRAINER"))
        store.set(CC.trainer)(to)
        executorFlow ++= featureFlow
        executorFlow += to.trainer
      case _ => println("mismatch")
    }
  }

  val pipeline = Function.chain(executorFlow)

  //input label rename
  pipeline(baseDf.withColumnRenamed("d14", "label"))

  //notice:version need spark2.3+
  val model = store.get(CC.trainer).get.Model
  val test = store.get(CC.spliter).get.test

  //transfer test-data
  val pipeline2 = Function.chain(featureFlow)
  pipeline2(test)

  val res = model.transform(store.get(CC.filter).get.dataset)
  res.show()
}
