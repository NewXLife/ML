/**
  * create by colin on 2018/9/4
  */
import util.SparkTools

case class Policy(d14: String, apply_date: String, day7: String, month1: String, month3: String, month6: String, month12: String, month18: String, month24: String, month60: String)

case class PSIT(colName:String, psiV:Double)

object TestPSI extends SparkTools {
  val sc = spark.sparkContext

  sc.setLogLevel("ERROR")


  val base = sc.textFile("/Users/colin/Desktop/bd_ML/docs/policy-test-ks-psi.csv").map(_.split(","))
    .map(p => Policy(p(0), p(1), p(2), p(3), p(4),
      p(5), p(6), p(7), p(8), p(9)))
  val predict = sc.textFile("/Users/colin/Desktop/bd_ML/docs/policy-test-ks-psi2.csv").map(_.split(","))
    .map(p =>Policy(p(0), p(1), p(2), p(3), p(4),
      p(5), p(6), p(7), p(8), p(9)))
  val label = "d14"
  val timeCol = "apply_date"
  val baseCount  = base.count()
  val baseDF = spark.createDataFrame(base)
  val b1 = baseDF.filter(x => x != baseDF.head())

  val predictDF = spark.createDataFrame(predict)
  val p1  = predictDF.filter(x => x != predictDF.head())

  val base1 = loadCSVData("csv","/Users/colin/Desktop/bd_ML/docs/policy-test-ks-psi.csv")
  base1.show(10)

  val pre = loadCSVData("csv","/Users/colin/Desktop/bd_ML/docs/policy-test-ks-psi2.csv")
  pre.show(10)


//  testPsi(b1, p1, label, timeCol).foreach(println(_))
  println("--------------------------------------")
  spark.createDataFrame(testPsi(base1, pre, label, timeCol)).show()




  /**
    * psi = sum[(Ac-Ex)*LN(Ac/Ex)]
    * @param baseDF
    * @param predDF
    * @param label
    * @param timeCol
    */
  def testPsi(baseDF: DataFrame, predDF: DataFrame,label: String,timeCol: String)= {
    val baseCount = baseDF.count()
    val cols = baseDF.schema.fieldNames.filterNot(x => x.equals(label) || x.equals(timeCol))

    var tempArray = new ArrayBuffer[PSIT]()

    for (c <- cols) {
      val baseSelectedCol = baseDF.select(col(c).cast(DoubleType))
      val (baseStartValues, baseCounts) = baseSelectedCol.rdd.mapPartitions(par=> par.map(v => v.getDouble(0))).histogram(10)
      val basePrecent = baseCounts.map(x => x.doubleValue() / baseCount)

      val predictSelectedCol = predDF.select(col(c).cast(DoubleType))
      val predCounts = predictSelectedCol.rdd.mapPartitions(par=> par.map(v => v.getDouble(0))).histogram(baseStartValues)
      val predPrecent = predCounts.map(x => x.doubleValue() / baseCount)


      println("col---->", c)
      for ((b,p) <- basePrecent.zip(predPrecent)){
        println(b,p)
//        var totalPsi = 0
//        val psi = (p - b) * Math.log(p/b)
//        totalPsi +=psi
      }

      val psi = predPrecent.zip(basePrecent).map(x => if (x._1 == x._2) 0 else (x._1 - x._2) * Math.log(x._1 / x._2) ).sum
      tempArray +=PSIT(c, psi.formatted("%.4f").toDouble)
    }

    tempArray
  }
}
