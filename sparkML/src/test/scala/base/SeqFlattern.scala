package base

import sta.Map2Json

object SeqFlattern extends App{
  val pattern = "[()\\[\\]]".r
  val testStr = "(1]"
  val res = pattern.replaceAllIn("(1]]", "")
  println(res)
//  import scala.collection.JavaConversions._
//  val testMap = Map2Json.getJavaMap.toMap
//  val rr = List("1")
//  println(testMap.keySet)
//  println(testMap.values)
//  println(testMap.values.flatten.toArray.mkString(","))
//  for(x  <- testMap.values.flatten){
//    println(x)
//  }
//  println(testMap.getClass)
}
