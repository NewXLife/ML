import util.SparkTools
case class KS(colName:String, ksV:Double)
object TestKS extends SparkTools {
  //  val data1 = spark.createDataFrame(Seq((0.1,0.03,0.3,0.25, 0.5)))
  //  val data2= spark.createDataFrame(Seq((0.3, 0.1, 0.12, 0.04, 0.32)))
  //
  //
  //  val a1= Array(0.1,0.03,0.3,0.25, 0.5)
  //  val a2 = Array(0.3, 0.1, 0.12, 0.04,0.32)
  //
  //  val s1 = a1.sorted
  //
  //  val s2 = a2.sorted
  //
  //  val total = s1 ++ s2
  //
  //  val res = searchSorted(s1, total).map(x => x/5.0)
  //
  //
  //  println("---------------------------------")
  //
  //  res.foreach(println(_))
  //
  //  val rr = ks2Samp(a1, a2)


  //  println("res---------:", rr.formatted("%.4f").toDouble)


  //  val tt = s1.zip(s2).map(x => x._2 - x._1)
  //  tt.foreach(println(_))
  val base1 = loadCSVData("csv", "E:\\NewX\\newX\\bd-engine\\docs\\20180814_old_tdbase.csv").limit(10)
  base1.show(10)

  val pre = loadCSVData("csv", "E:\\NewX\\newX\\bd-engine\\docs\\20180907_old_td.csv").limit(10)
  pre.show(10)



  def testKS(baseDf:DataFrame, preDf:DataFrame, cols:Array[String]) ={
    var tempArray = new ArrayBuffer[KS]()
    for ( c <- cols){
      val data1 = baseDf.select(col(c).cast(DoubleType)).map(x => x.getDouble(0)).collect()
      val data2 = preDf.select(col(c).cast(DoubleType)).map(x => x.getDouble(0)).collect()
      tempArray += KS(c, ks2Samp(data1, data2))
    }
    tempArray
  }


  def ks2Samp(data1: Array[Double], data2: Array[Double]) = {
    val s1 = data1.sorted
    val s2 = data2.sorted
    val n1 = s1.length
    val n2 = s2.length

    val dataAll = s1 ++ s2

    val cdf1 = searchSorted(s1, dataAll).map(x => x / (1.0 * n1))
    val cdf2 = searchSorted(s2, dataAll).map(x => x / (1.0 * n2))

    val statistic = cdf1.zip(cdf2).map(x => x._2 - x._1).map(x => math.abs(x)).max
    statistic
  }


  private def searchIndex(v2: Double, array: Array[Double]): Int = {
    var temp = 0
    for (i <- array.indices) if (v2 >= array(i)) temp += 1 else return temp
    temp
  }

  /**
    *
    * @param data1 ,data2 sequence of 1-D ndarrays,two arrays of sample observations assumed to be drawn from a continuous,distribution, sample sizes can be different
    * @param side
    * @return
    */
  def searchSorted(data1: Array[Double], data2: Array[Double], side: String = "right") = {
    require(data1.length > 0 && data2.length > 0, "Array size must be bigger than zero")
    val len1 = data1.length
    val len2 = data2.length

    var resIndexArray: ArrayBuffer[Int] = new ArrayBuffer[Int]()
    val r1 = (for (i <- data1.indices) yield i.toInt).toArray

    resIndexArray ++= r1.map(x => x + 1)

    for (j <- 0 until (len2 - len1)) {
      val v2 = data2(j + len1)
      resIndexArray += searchIndex(v2, data1)
    }
    resIndexArray
  }
}
