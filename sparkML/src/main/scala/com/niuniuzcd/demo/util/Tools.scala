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
}
