package ml.feature.encoder

import org.apache.spark.ml.feature.{StringIndexer, StringIndexerModel, VectorAssembler}
import util.SparkTools

object BaseEncoderTest extends SparkTools {
//  val be = new BaseEncoder(0.8, 0.8, 0.9)
//  be.fit(baseDf)
//  val res = be.transform(baseDf)

//  val ce = new CategoryEncoder(1, true, 1, true)
//  ce.fit(baseDf.select("ad"))
//  val resce = ce.transform(baseDf.select("ad"))

  baseDf.show(10, truncate = false)
//  res.show(10, truncate = false)


//  val vs  = new VectorAssembler().setInputCols(Array("ad"))
//    .setOutputCol("feature")
//
//  val feaVs = vs.transform(baseDf)

  val strIndex = new StringIndexer()
    .setInputCol("ad")
    .setOutputCol("ad_new")
    .fit(baseDf)
  val res = strIndex.transform(baseDf)

  res.show(10000, truncate=false)
  res.drop("ad").withColumnRenamed("ad_new","ad").show(1000, truncate = false)
//  val strIndexM = new StringIndexerModel()
}
