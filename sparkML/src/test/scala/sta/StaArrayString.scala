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
}
