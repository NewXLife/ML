package com.niuniuzcd.demo.base

class U{
  def prin = println("hello")

}

object UU

class Test {
  class T{

  }

  val t = new T
  t.getClass
  classOf[T]

  import scala.reflect.runtime.universe._
  typeOf[T]

  import scala.reflect.ClassTag
  def mkArray[T: ClassTag](elems: T*) = Array(elems :_*)
  mkArray(1,2)
  mkArray("hello", "java")

  val sttt = new U
  testsubClass(sttt)

  def testsubClass[T <: U](first: T) = {
    println(first.prin)
  }
}
