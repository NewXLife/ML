package ml

import org.apache.spark.sql.SparkSession


object ModelSelectAndTuning extends App{

      val spark = SparkSession.builder().master("local[*]").appName("test").getOrCreate()

      //define training
      val training = spark.createDataFrame(Seq(
        (0L, "a b c d e spark", 1.0),
        (1L, "b d", 0.0),
        (2L, "spark f g h", 1.0),
        (3L, "hadoop mapreduce", 0.0),
        (4L, "b spark who", 1.0),
        (5L, "g d a y", 0.0),
        (6L, "spark fly", 1.0),
        (7L, "was mapreduce", 0.0),
        (8L, "e spark program", 1.0),
        (9L, "a e c l", 0.0),
        (10L, "spark compile", 1.0),
        (11L, "hadoop software", 0.0)
      )).toDF("id", "text", "label")

  training.show()

//      val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
//
//      val hashingTf = new HashingTF().setInputCol(tokenizer.getOutputCol).setOutputCol("features")
//
//      val lr = new LogisticRegression().setMaxIter(10)
//
//      //define pipeline
//      val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTf, lr))
//
//
//      val paramGrid = new ParamGridBuilder().addGrid(hashingTf.numFeatures, Array(10, 100, 1000)).addGrid(lr.regParam, Array(0.1, 0.01)).build()
//
//
//      val cv = new CrossValidator().setEstimator(pipeline).setEvaluator(new BinaryClassificationEvaluator).setEstimatorParamMaps(paramGrid).setNumFolds(2)
//
//      val cvModel = cv.fit(training)
//
//      val test = spark.createDataFrame(Seq(
//        (4L, "spark i j k"),
//        (5L, "l m n"),
//        (6L, "mapreduce spark"),
//        (7L, "apache hadoop")
//      )).toDF("id","text")
//
//      cvModel.transform(test).select("id", "text", "probability", "prediction").collect()
//    .foreach{
//      case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
//        println(s"($id, $text) --> prob=$prob, prediction=$prediction")
//    }
}
