package com.niuniuzcd.demo.ml.mlHandler

import org.scalatest.FunSuite

/**
  * @author zcd
  */
class MLEngineTest extends FunSuite {
  //定义一个柯里函数
  def withCallBack[T, U](name: String, test: T)(action: T => U) = {
    try {
      action(test)
    } catch {
      case e: Exception =>
        throw e
    }
  }

  test("test-function") {
    val tleng = countTest("hello", "scala", "spark")
    println("input str length:", tleng)

    val numx = countTest2(1, 2, 3, 4)
    println("input number res: ", numx)
  }

  def reStr(inputStr: String*): String = {
    inputStr.mkString(",")
  }

  def reInteger(inputStr: Int*) = {
    var sum = 0
    for (x <- inputStr) sum += x
    sum
  }

  def countTest(str: String*) = withCallBack("count", reStr(str: _*))(input => "<<" + input + ">>")

  def countTest2(number: Int*) = withCallBack("count", reInteger(number: _*))(input => input * 2)
}
