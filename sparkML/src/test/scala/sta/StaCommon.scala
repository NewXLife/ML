package sta

object StaCommon extends App{
  val t = s"stack(2, 'm1', m1, 'm3', m3) as (feature, value)"
  def getStackParams(s1: String, s2:String*):String ={
    val buffer = StringBuilder.newBuilder
    var size = 0
    if(s1 != null) size =1
    size += s2.length
    buffer ++= s"stack($size, '$s1', $s1"
    for(s <- s2) buffer ++= s",'$s', $s"
    buffer ++= ")"
    buffer.toString()
  }

  def getStackParams(s2:String*):String ={
    val buffer = StringBuilder.newBuilder
    var size = 0
    size += s2.length
    buffer ++= s"stack($size "
    for(s <- s2) buffer ++=  s",'$s', $s"
    buffer ++= ")"
    buffer.toString()
  }


  val columns = Array("s1", "s2", "s3")
  println(getStackParams(columns:_*))
}
