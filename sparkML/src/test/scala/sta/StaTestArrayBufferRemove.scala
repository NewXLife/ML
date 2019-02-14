package sta

import scala.collection.mutable.ArrayBuffer

object StaTestArrayBufferRemove extends App {
  val testArray = ArrayBuffer("1","2","3","4")
  val excludeCol = Array("1","2")
  for( col <- excludeCol) testArray.remove(testArray.indexOf(col))

  println(testArray.mkString(","))
}
