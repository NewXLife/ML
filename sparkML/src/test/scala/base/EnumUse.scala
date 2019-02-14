package base

/**
  * scala enum use
  */
object EnumUse extends Enumeration {
 type EnumUse = Value
  val V1, V2, V3 = Value
}

object UseEnum {
  import EnumUse._
  def test1 (e: EnumUse) = {
     e match {
       case V1 => println("handle v1")
       case V2 => println("handle v2")
       case _ => println("default ...")
     }
  }
}
