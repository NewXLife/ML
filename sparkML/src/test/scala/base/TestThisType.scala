package base

object TestThisType extends App{
  val test = new Test2
  test.method1.method2
}

private[base] class Test1 {
  def method1:this.type = this
}

private[base] class Test2 extends Test1 {
  def method2:this.type  = this
}
