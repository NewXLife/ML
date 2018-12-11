package sta

object StaCommon extends App{
  val t = s"stack(2, 'm1', m1, 'm3', m3) as (feature, value)"
  def getStackParams(s1: String, s2:String*):String ={
    val buffer = StringBuilder.newBuilder
    var size = 0
    if(s1 != null) size =1
    size += s2.length
    buffer ++= s"stack($size, '$s1', $s1"
    for(s <- s2) buffer ++= s",'$s', $s"
    buffer ++= ")"
    buffer.toString()
  }

  def getStackParams(s2:String*):String ={
    val buffer = StringBuilder.newBuilder
    var size = 0
    size += s2.length
    buffer ++= s"stack($size "
    for(s <- s2) buffer ++=  s",'$s', $s"
    buffer ++= ")"
    buffer.toString()
  }

//  val columns = Array("s1", "s2", "s3")
//  println(getStackParams(columns:_*))

  val testArray = Array(Double.NegativeInfinity, 20.0, 28.0, 34.0, 40.0, 46.0, 53.0, 60.0, 69.0, 82.0, Double.PositiveInfinity)
  val testNumb = 53.0d
 println( testArray.indices)
  //(a, b]


  def searchIndex(v2: Double, array: Array[Double]): Int = {
    var temp = 0
    for (i <- array.indices) if (v2 >= array(i)) temp += 1 else return temp
    temp
  }

  val index = searchIndex2(100, testArray)
  println(testArray(index-1), testArray(index))
  def searchIndex2(v2: Double, array: Array[Double]): Int = {
    var temp = 0
    for (i <- array.indices) if (v2 > array(i)) temp += 1 else  temp
    temp
  }

}
