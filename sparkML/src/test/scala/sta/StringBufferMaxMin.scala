package sta

object StringBufferMaxMin extends App{
val t = "1,2,3,4,4,7,1,100,10,2"
  val s = t.split(",").map( x=> x.toDouble)
  println(s.getClass.getSimpleName)
  println(s.max)
  println(s.min)
}
