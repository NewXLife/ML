package base

object SeqFlattern extends App{
//  val pattern = "[()\\[\\]]".r
//  val testStr = "(1]"
//  val res = pattern.replaceAllIn("(1]]", "")

  val testArray = Array("f1","f2","f3").toBuffer

  //获取移除的元素，此时 testArray = Array("f1","f3")
  val res = testArray.remove(testArray.indexOf("f2"))

  val r1 = testArray.filterNot(x => x.equals("f1"))

  println(res)
  println(testArray.mkString(","))
  println(r1.mkString(","))

}
