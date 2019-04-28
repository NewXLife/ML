package base.scala_base_language

import org.scalatest.FunSuite

/**
  * @author zcd
  */
class ScalaLazyTest extends FunSuite {
  test("lazy-features") {
    def loop[A](n: Int)(body: => A) = (0 until n) foreach (n => body)

    loop(2)("hello")
    println(0 until 10)
  }

  /**
    * 求值策略call by value；先求值在传递参数
    *
    * @param x
    * @return
    */
  def callByValue(x: String): String = {
    println(x)
    x
  }

  /**
    * 求值策略call by name；函数形参数以 => 开头
    * 函数参数每次在函数体内被使用时都会被求值
    *
    * @param x
    * @return
    */
  def callByName(x: => String): String = {
    println(x)
    x
  }

  test("call-function-type") {
    callByValue("hello")
    callByName(callByValue("hello") + "scala")
    //hello---->t1("hello") 输出
    //hello---->println(x) 此时的x是 t1("hello") + "scala"，由于是call-by-name；在使用的时候才执行，因此t1("hello")  打印 "hello",返回hello；
              //println("helloscala")-> 打印helloscala
    //helloscala
    //hello   // 返回x，此时的x是 t1("hello") + "scala"，x被调用，x被执行，t1("hello")的“hello”被打印
  }
}
