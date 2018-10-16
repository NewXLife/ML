object MatchTest1 extends App{
val user = new FreeUser("tiger", 3000, 0.8d)
  user match {
    case freeUser @ preCheck() => init(freeUser)
    case _ => println("hello")
  }

  def init(user: FreeUser): Unit ={
    println(user.upgrade, user.name, user.score)
  }
}




trait User{
  def name:String
  def score:Int
}

class FreeUser(val name:String = "", val score:Int , val upgrade:Double) extends User{
}

class PreUser(val name:String, val score:Int) extends User

object FreeUser{
  def unapply(user: FreeUser): Option[(String, Int, Double)] = Some((user.name,user.score, user.upgrade))
}

object PreUser{
  def unapply(arg: PreUser): Option[(String, Int)] = Some((arg.name, arg.score))
}

object preCheck{
  def unapply(arg: FreeUser): Boolean = arg.upgrade > 0.75
}

object Test{
  val name = 2
  def name2 :Int = name
}

