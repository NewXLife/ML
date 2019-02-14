package com.niuniuzcd.demo.util

/**
  * create by colin on 2018/8/28
  */
object Tools {
  def toHash(key:String): Int = {
    val arraySize = 19919
    var hashCode = 0
    for(i <- 0 until key.length){
      val letterValue = key.charAt(i) - 96
      hashCode = ((hashCode << 5) + letterValue) % arraySize
    }
    hashCode
  }

  def filterSpecialChar(str:String): String = {
    //    val pattern = "[`~!@#$%^&*()+=|{}':;'\\[\\]<>/?~！@#￥%……&*（）——+|{}【】‘；：”“’。、？]".r
    val pattern = "[`~!@#$%^&*()+=|{}':'\\<>/?~！@#￥%……&*（）——+|{}【】‘；：\"”“’。、？]".r
    pattern replaceAllIn(str, "")
  }



  def getStackParams(s1: String, s2: String*): String = {
    val buffer = StringBuilder.newBuilder
    var size = 0
    if (s1 != null) size = 1
    size += s2.length
    buffer ++= s"stack($size, '$s1', $s1"
    for (s <- s2) buffer ++= s",'$s', $s"
    buffer ++= ")"
    buffer.toString()
  }

  def getStackParams(s2: String*): String = {
    val buffer = StringBuilder.newBuilder
    var size = 0
    size += s2.length
    buffer ++= s"stack($size "
    for (s <- s2) buffer ++= s",'$s', $s"
    buffer ++= ")"
    buffer.toString()
  }
}
