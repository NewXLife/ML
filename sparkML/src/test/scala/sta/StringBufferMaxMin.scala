package sta

import scala.util.Success

object StringBufferMaxMin extends App{
//val t = "1,2,3,4,4,7,1,100,10,2"
//  val s = t.split(",").map( x=> x.toDouble)
//  println(s.getClass.getSimpleName)
//  println(s.max)
//  println(s.min)

  val labelName = "10d"
  val featureArray = Array("td","1d","7d").toBuffer
  if(featureArray.exists(name => labelName.contains(name)))featureArray.remove(featureArray.indexOf(labelName))
  println(featureArray.mkString(","))

  val ttt = "2018-06-24 00:00:00"

  val r1=scala.util.Try(ttt.toDouble)
  val result = r1 match {
    case Success(_) => "Int" ;
    case _ =>  "NotInt"
  }
}
