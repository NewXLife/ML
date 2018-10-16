import org.scalatest.FunSuite

/**
  * create by colin on 2018/8/16
  */
object ClassCastTest  extends  FunSuite{

  /**
    * 第一种类型转换方式
    *
    * @param s string
    * @return 转换后的类
    */
  def parseDouble(s: String): Option[Double] =
    try { Some(s.toDouble) }
    catch { case _ => None }

  /**
    * 第二种类型转换方式
    *
    * @param s   string
    * @tparam T  class type
    * @return option
    */
  def parse[T: ParseOp](s: String): Option[T] = try { Some(implicitly[ParseOp[T]].op(s)) }  catch {case _ => None}
  /* 隐式转换类型类*/
  case class ParseOp[T](op: String => T)
  implicit val popDouble = ParseOp[Double](_.toDouble)
  implicit val popInt = ParseOp[Int](_.toInt)
  implicit val popLong = ParseOp[Long](_.toLong)
  implicit val popFloat = ParseOp[Float](_.toFloat)


}
