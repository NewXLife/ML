package sta

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object BufferTest extends App{
  val splits = Seq(-1, -3, -4, 0, 0, 0, 1,2,3,4,6.0)
  val res = splits.filter(_ >0)
  println(res)
  var buffer = res.toBuffer
  if(buffer.head>0) buffer = Double.NegativeInfinity +: buffer
  buffer(buffer.length - 1) = Double.PositiveInfinity
  val t = buffer.toArray
  println(t.mkString(","))


  val ls = ListBuffer(1,2,3)
  val  uu = 3 +: ls
  println(uu.mkString(","))
}
