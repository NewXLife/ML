class B
val b = new B
b.getClass
classOf[B]

import scala.reflect.runtime.universe._
typeOf[B]

import scala.reflect.ClassTag
def mkArray[T: ClassTag](elems: T*) = Array(elems:_*)
mkArray(1,2)
mkArray("hello","scala")

val t = 3
val s = 4
val m = t match {
  case 3 if s <=4 =>  3
  case 3 => 100
}
m