import com.kuainiu.beidou.domain.data.ds.{DSType, Sample}

import scala.collection.JavaConversions._

object LivyScala extends  App {

//  val sample = new Sample()
val indexId = toHash("latest_borrow_s1an") + 100.hashCode() * 17 + 100.hashCode() * 13

  println(indexId)

  def toHash(key:String) = {
    val arraySize = 19919
    var hashCode = 0
    for(i <- 0 until key.length){
       val letterValue = key.charAt(i) - 96
      hashCode = ((hashCode << 5) + letterValue) % arraySize
    }
    hashCode
  }

// sample.setDsType(DSType.dataset)
//  val t = sample.getDsType
//
//  println(t.getClass.getSimpleName)
//  println (t)
}
