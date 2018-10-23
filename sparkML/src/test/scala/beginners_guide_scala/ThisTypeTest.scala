package beginners_guide_scala

object ThisTypeTest extends App{
  val test = new Test2
  test.method1.method2
}

/**
  * private[包名]包名可以是父包名或当前包名，如果是父包名，则父包和子包都可以访问
  * private[this]修饰的方法或字段只能在本类访问，如果是字段编译成java的时候就没有get或set方法。
  */
private[beginners_guide_scala] class Test1 {
  def method1:this.type = this
}

private[beginners_guide_scala] class Test2 extends Test1 {
  def method2:this.type  = this
}
