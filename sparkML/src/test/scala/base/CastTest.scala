package base

import org.scalatest.FunSuite

import scala.util.Try

/**
  * create by colin on 2018/8/16
  */
object CastTest  extends  FunSuite{

  /**
    * 第一种类型转换方式
    *
    * @param s string
    * @return 转换后的类
    */
  def parseDouble(s: String): Try[Option[Double]] =
    Try(Some(s.toDouble))

  /**
    * 第二种类型转换方式
    *
    * @param s   string
    * @tparam T  class type
    * @return option
    */
  def parse[T: ParseOp](s: String): Try[Option[T]] = Try(Some(implicitly[ParseOp[T]].op(s)))

  /* 隐式转换类型类*/
  case class ParseOp[T](op: String => T)
  implicit val popDouble: ParseOp[Double] = ParseOp[Double](_.toDouble)
  implicit val popInt: ParseOp[Int] = ParseOp[Int](_.toInt)
  implicit val popLong: ParseOp[Long] = ParseOp[Long](_.toLong)
  implicit val popFloat: ParseOp[Float] = ParseOp[Float](_.toFloat)


}
