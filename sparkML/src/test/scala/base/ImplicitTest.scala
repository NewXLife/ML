package base

import org.scalatest.FunSuite

/**
  * 引入隐式转换
  * 1.从源或者目标类型的伴生对象中的隐式函数
  * 2.位于当前作用域可以以当个标识符指代的隐式函数
  *
  * 转换规则
  * 1. 表达式的类型和预期类型不同的时候
  * 2. 当访问一个不存在的成员时候
  * 3. 调用某个方法，而该方法的参数声明与传入的参数不匹配时
  *
  * 三种不会尝试隐式转换
  * 1. 代码在不使用隐式转换的前提下，能够通过编译，则不会使用隐式转换；（如果a * b 能够编译，那么编译器不好尝试a * convert(b) 或者 convert(a) * b
  * 2. 编译器不会尝试执行多个转换,比如 convert1(convert2(a)) * b
  * 3. 存在二义性的转换，如果convert(a) * b 和 convert2(a) * b 都是合法的，编译器将会报错。
  */
class ImplicitTest extends FunSuite {

  /**
    * 函数和方法和带有一个标记为implicit的参数列表，这样编译器会查找缺省值，提供给函数和方法
    */
  test("implicit parameters") {
    /**
      * 对于给定的数据类型，只能有一个隐式的值，使用基础类型的隐式参数不是一个好的idea
      */
    //例如：def quote(str: String)(implicit left: String, right: String) //不要这样做, 因为你没法提供两个不同的字符串，这儿提供字符串都隐式转换为一个值

    // 声明隐式参数，不需要名称相同，只能类型相同就可以了
    // 下面表示的int类型将会转换为常量10
    implicit val c: Int = 10

    //
    def add(implicit a: Int, b: Int): Int = a + b

    println(add) //20 ；a, b都被隐式转换为10
    //显示调用
    println(add(3, 4)) //7

    def richAdd(a: Int)(implicit aa: Int, bb: Int) = a + aa + bb

    println(richAdd(3)) //23

    //curried函数的隐式参数一定要作为最后一个参数
    def plus(a: Int)(implicit b: Int): Int = a + b

    println(plus(3)) //缺省的时候用隐式值  //13
    //显示调用
    println(plus(3)(4)) // 7

    //单元测试
    println(plus(3))
    assertResult(plus(5))(15)
    assert(plus(5)(15) === 20)
    println(add(3, 3))


    case class Delimiters(left: String, right: String)
    implicit val defaultDeli: Delimiters = Delimiters("->", "<-")

    def quote(str: String)(implicit delims: Delimiters) = {
      delims.left + str + delims.right
    }

    //显示调用
    val res = quote("test")(Delimiters("<<", ">>"))
    println(s"---------res:$res")

    //implicit val t: Delimiters = Delimiters("(",")")

    //    @implicitNotFound(msg = "cannot found ")

    val c1 = "a".map(_.toString)
    val c2 = "a".map(_.toInt)
    println(c1.getClass.getSimpleName)
    println(c2.getClass.getSimpleName)
    val res2 = quote("test2") //省略隐式参数列表,编译器将会从两个地方查找这样的对象
    //当前作用域所有可以用单个标识符指代的，满足类型要求的val 和def
    //所要求的类型的伴生对象，或者关联类型包含类型本身，以及它的类型参数
    println(res2)

  }

  //利用隐式参数进行隐式转换
  test("use implicit parameters transformer") {
    //order是一个带有单个参数的函数，并且有implicit标签，并且有一个单个标识符出现的名称，因此它不仅是一个隐式参数，还是一个隐式转换
    def smaller[T](a: T, b: T)(implicit order: T => Ordered[T]) = {
      if (a > b) a else b //如果 a 没有带 > 操作符合的时候，将调用  order(a) < b
    }

    assert(smaller(20, 3) === true)
  }

  //上下文界定
  test("context border") {
    //要求作用域中存在一个类型为Ordering[T] 的隐式值
    class Pair[T: Ordering](val s1: T, val s2: T) {
      def smaller(a: T, b: T) = {
        import Ordered._
        if (a > b) a else b
      }

      //该隐式值可以被用在该类的方法中
      def smaller2(implicit ord: Ordering[T]) = {
        println(s"smaller2 ord className:${ord.getClass.getSimpleName}")
        println(s"smaller2 ord value:$ord]")
        if (ord.compare(s1, s2) > 0) s1 else s2
      }

      //使用implicitly方法获取隐式值
      def testSmaller = {
        val opt = implicitly[Ordering[T]]
        println(s"testSmaller ord className:${opt.getClass.getSimpleName}")
        println(s"testSmaller ord value:$opt]")
        //        if( opt > s1) s1 else s2
      }

      def test2Smaller(ord: Ordering[T]) = {
        if (ord.compare(s1, s2) > 0) s1 else s2
      }
    }

    //new Pair(40,2) 编译器推断出我们需要一个Pair[Int]，由于predef 作用域中有一个类型为Ordering[Int]的隐式值，一次Int满足上下文界定。
    //这个Ordering[Int]就成为该类的一个字段，被传入到该值的方法中。
    val p = new Pair(40, 2).smaller2
    val p1 = new Pair(40, 2).testSmaller
    val p2 = new Pair(40, 2).test2Smaller(Ordering[Int])

    //e.g
    trait Monoid[A] {
      def mappend(a: A, b: A): A

      def mzero: A
    }
    object Monoid {
      implicit val intMonoid: Monoid[Int] = new Monoid[Int] {
        def mappend(a: Int, b: Int): Int = a + b

        def mzero: Int = 0
      }
      implicit val strMonoid: Monoid[String] = new Monoid[String] {
        def mappend(a: String, b: String): String = a + b

        def mzero: String = ""
      }
    }

    def sumMonoid[A](xs: List[A], a: Monoid[A]) = xs.foldRight(a.mzero)(a.mappend)

    // next
    def sumMonoid2[A](xs: List[A])(implicit a: Monoid[A]) = {
      xs.foldRight(a.mzero)(a.mappend)
    }

    // next
    def sumMonoid3[A: Monoid](xs: List[A]) = {
      val m = implicitly[Monoid[A]]
      xs.foldRight(m.mzero)(m.mappend)
    }

    val testList = List(1, 2, 3, 4)
    val testList2 = List("hello", "c", "java")
    println(sumMonoid3(testList)) //作用域查找是否存在Monoid[A]的隐式值，在伴生对象中找到了，因此存在
    println(sumMonoid3(testList2))
    println(sumMonoid2(testList2))
    println(sumMonoid2(testList))
  }


  test("Invoking head on an empty Set should produce NoSuchElementException") {
    intercept[NoSuchElementException] {
      Set.empty.head
    }
  }

  test("implicit function") {
    class UT(s: Int, t: Int) {
      def plus(): Int = s + t
    }
    object UT {
      def apply(s: Int, t: Int): UT = new UT(s, t)
    }
    //隐式转换函数：以implicit关键字声明的带有单个参数的函数，这样的函数将会自动应用，将值从一种类型转换另外一种类型
    implicit def int2Bigger(n: Int): UT = new UT(n, 1)

    val res = 2.plus()
    assert(res === 3)
  }

  test("class define implicit function") {
    //如果你应用某些框架，或者好的源码，但是发现并没有提供你想要的方法，你可以用implicit 来丰富它
    //假如int 类型没有提供plus 和minus 方法，你可以写一个类丰富它
    //step1:定义一个拥有丰富的方法的类型
    class RichNumbers(val t: Int) {
      def plus(a: Int, b: Int): Int = a + b
      def minus(a: Int, b: Int): Int = a - b
    }

    object RichNumbers {
      def apply(t: Int): RichNumbers = new RichNumbers(t)
      //step2:提供一个隐式转换将原来的类型转换为新的类型
      implicit def int2r(a: Int): RichNumbers = new RichNumbers(a)
    }

    /**
      * 某个隐式转换带来麻烦，排除在外
      */
    //import RichNumbers.{int2r => _, _}

    // 隐式转换
    import RichNumbers._
    //step3:这样就可以在Int 类型上面调用新方法了，它被隐式的转换为RichNumbers类型
    val temp = 10.minus(10, 1)
    val temp1 = 10.plus(1, 2)
    assert(temp === 9)
    assert(temp1 === 3)
  }

  test("type prove"){
    // T =:= U  T等于U
    // T <:< U  T是否是U的子类
    // T <%< U  T可以被隐式转换U
//    def firstLast[T, U](it: T)
  }

  test("string2double") {
    val t = Array("-Infinity", "0.0", "1.0", "2.0", "3.0", "4.0", "5.0", "6.0", "7.0", "Infinity")
    val s = t.map(x => x.toDouble)
    println(s.mkString(","))
  }
}
