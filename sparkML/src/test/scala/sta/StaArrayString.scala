package sta

import scala.util.matching.Regex

object StaArrayString extends App{
  val str = "[[1,2,3]];[[0,3,5,6]]"
  val sp = str.split(";")
  println(sp(0))
  println(sp(1))

  def filterSpecialChar(str:String): String = {
    //    val pattern = "[`~!@#$%^&*()+=|{}':;'\\[\\]<>/?~！@#￥%……&*（）——+|{}【】‘；：”“’。、？]".r
    val pattern = "[\\[\\])]".r
    pattern replaceAllIn(str, "")
  }
  println(filterSpecialChar(str))

  val inttt = "0.0"
  println(inttt.toDouble.toInt)
  println(inttt.toDouble)
  if(inttt.toDouble == 0d)
    {
      println("o")
    }else{
    println("e")
  }

  def searchIndex2(v2: Double, array: Array[Double]): Int = {
    var temp = 0
    for (i <- array.indices) if (v2 > array(i)) temp += 1 else temp
    temp
  }

  val testArray= Array(Double.NegativeInfinity, 2.0, 5.0, 7.0, 10.0, Double.PositiveInfinity)
  println(searchIndex2(9.0,testArray))
}
