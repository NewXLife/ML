package base

import org.scalatest.FunSuite

class UT(s: Int, t: Int) {
  def plus() = {
    s + t
  }
}

object UT {
  def apply(s: Int, t: Int): UT = new UT(s, t)
}

/**
  * 引入隐式转换
  * 1.从源或者目标类型的办事对象中的隐式函数
  * 2.位于当前作用域可以以当个标识符指代的隐式函数
  *
  * 转换规则
  * 1. 表达式的类型和预期类型不同的时候
  * 2. 当访问一个不存在的成员时候
  * 3. 调用某个方法，而该方法的参数声明与传入的参数不匹配时
  *
  * 三种不会尝试隐式转换
  * 1. 代码在不使用隐式转换的前提下，能够通过编译，则不好使用隐式转换
  * 2. 编译器不好尝试执行多个转换
  * 3.存在二义性的转换
  */

class ImplicitTest extends FunSuite {
  test("implicit") {
    //  调用时无法省略参数
    def add(implicit a: Int, b: Int): Int = a + b

    //  一定要作为最后一个参数
    def plus(a: Int)(implicit b: Int): Int = a + b

    //  声明隐式参数，不需要名称相同，只能类型相同就可以了
    implicit val c: Int = 10
    //  单元测试
    assertResult(plus(5))(15)
    assert(plus(5)(15) === 20)
  }

  test("Invoking head on an empty Set should produce NoSuchElementException") {
    intercept[NoSuchElementException] {
      Set.empty.head
    }
  }

  test("implicit0001") {
    //隐式转换函数：以implicit关键字声明的带有单个参数的函数，这样的函数将会自动应用，将值从一种类型转换另外一种类型
    implicit def int2Bigger(n: Int): UT = new UT(n, 1)

    val res = 2.plus()
    assert(res === 3)
  }


  class RichNumbers(val t: Int) {
    def plus(a: Int, b: Int) = {
      a + b
    }

    def minus(a: Int, b: Int) = {
      a - b
    }

    object RichNumbers {
      def apply(t: Int): RichNumbers = new RichNumbers(t)

      //提供一个隐式转换将原来的类型转换为新的类型
      implicit def int2r(a: Int) = new RichNumbers(a)
    }


    test("implicit use step") {
      //如何你应用某些框架，或者好的源码，但是发现并没有提供你想要的方法，你可以用implicit 来丰富它
      //定义一个拥有丰富的方法的类型

      /**
        * 某个隐式转换带来麻烦，排除在外
        */
      //import RichNumbers.{int2r => _, _}

      //这样就可以在Int 类型上面调用新方法了，它被隐式的转换为RichNumbers类型

      /**
        * 隐式转换
        *
        */
      import RichNumbers._
      val temp = 10.minus(10, 1)
      val temp1 = 10.plus(1, 2)
      assert(temp === 9)
      assert(temp1 === 3)
    }

    test("float2 double"){
      val t = Array("-Infinity","0.0","1.0","2.0","3.0","4.0","5.0","6.0","7.0","Infinity")
      val s = t.map(x => x.toDouble)
      println(s.mkString(","))
    }

  }
}
