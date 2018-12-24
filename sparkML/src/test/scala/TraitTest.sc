case class User(fName:String, lName: String, score:Int)

def advance(xs: List[User]) = xs match {
  case User(_, _, score1):: User(_, _, score2):: _ => score1 - score2
  case _ => 0
}

val u = User("tiger", "lastTiger", 100)

val u1= User("first", "ttrt", 99)

val l = List(u, u1)
advance(l)


//trait
/**在Scala中，Trait可以定义具体字段，继承Trait的类就自动获取了Trait中定义的类。
  注意：这里与继承Class不同，如果继承Class获取的字段，实际定义在父类中，而继承Trait获取的字段，就直接添加到了类中
  */
trait  A{
  def t:String
  val name:String ="hello"
  def tt(x:Any):Boolean = true
}

trait B{
  def t(name:String)
}

class FreeA(val t:String) extends A {
}

val f = new FreeA("hello")
f.name
f.t

class PA(val t:String) extends A

class cc extends A{
  def t ={
    "tiger"
  }
}


//case class
//注意，当一个类被生成为case class 时，scala会默认作如下工作：
//（1）如果参数不加var/val修改，默认为val。
//（2）自动创建伴生对象，实现apply方法，方便了我们在创建对象时不适用new
//（3）实现自己的toString、hashCode、copy和equals方法
//mut person
abstract class Person
case class Student(name:String, sex:String) extends Person
case class Teacher(name:String, sex:String) extends Person

case class SchoolPerson(desc:String, person: Person*)

val sp = SchoolPerson("test",Student("tiger", "test01"), Teacher("t1", "test"))

sp match {
  case SchoolPerson(_, Student(name, sex), _) => println("name:" + name + "sex:"+sex)
  case SchoolPerson(_, Teacher(name, sex)) => println("tname:"+ name+ "tsex:"+ sex)
  case _ => println("no")
}

def caseMatch(p:Person) = p match {
  case Student(name,sex) => println(name, sex)
  case Teacher(tname,tsex) => println(tname, tsex)
  case _ => "no match"
}

//深度copy
val t = Teacher("ttest", "tsex")
println("name->"+t.name, "sex->"+t.sex)
//(name->ttest,sex->tsex)
val t1 = t.copy()
println(t1.name, t1.sex)
//(ttest,tsex)
println(t == t1)
//true

val t2 = t.copy(name ="xx") // val t3 = t.copy(sex ="tt") // val t4 = t.copy(name="1", sex="test")
println(t2 == t)
//false


val mAsTuple:(String, String) = ("t1","t2")

Student.tupled(mAsTuple)

val transform: Student => Option[(String, String)] ={
  Student.unapply
}

val ss = Student("hello","tiger")
transform(ss)

case class TT(name: String) (p1 :String, p2:String)

val ut2 = TT("tiger")("hello","tiger")
ut2.name


//部分应用和偏函数
def tiger(x:Int)(y:Int) = x+ y  // f(1)(3)
def tiger2(x:Int, y:Int) = x + y  //f(1,3)
def tiger3 = (x:Int) => (y:Int) => x+y
// tiger3 会先接受一个参数x  然后返回一个 f(y) = 1 + y
//再输入参数就 变为   f(3) = 1 + 3

val tigerT1 = tiger(1)_ // f(y) = 1 + y
val tiger3T1 = tiger3(1) // f(y) = 1 + y

val cFuntion = (tiger2 _).curried(2)(3)
//(tiger2 _).curried  等价于 tiger(x:Int)(y:Int) = x+y
//(2)(3)  ==  f(2)(3)

tigerT1(3)
tiger3T1(3)


//trait partII
/**
  * Trait调用链：在Scala中，支持让类继承多个Trait，依次调用多个Trait中的同一个方法，只要让多个Trait中的同一个方法中，在最后都执行super方法。
  * 注意：在类中调用多个Trait中都有的方法时，首先会从最右边的Trait的方法开始执行，然后依次向左执行，形成一个调用条。这个相当于设计模式中的责任链模式的一种具体实现依赖
  *
  */
trait Handler{
  def handle(data:String){println("p:"+ data)}
}

trait v1 extends Handler{
  override def handle(data: String): Unit = {
    println("v1 input data:" + data)
//    super.handle(data) //如果将这里进行注释掉，调用多个trait都有的方法将会断链接，因此后面的执行将会仅仅到这里，不会执行其它到trait
  }
}


trait v2 extends Handler{
  override def handle(data: String): Unit = {
    println("v2 input data:" + data)
    super.handle(data)
  }
}

trait v3 extends Handler{
  override def handle(data: String): Unit = {
    println("v3 input data:" + data)
    super.handle(data)
  }
}


class Thandler(name :String) extends v2 with v1 with v3{
  def sasHello = println("hello:" + name)
  handle(name)
}

/**
  * v3 input data:tiger
  * v1 input data:tiger
  * 后面没有执行了上因为再v1 没有使用super.handle(data) ,断链了
  * Tu: Thandler = Thandler@c2b4011
  * hello:tiger
  * res13: Unit = ()
  */

val Tu = new Thandler("tiger")
Tu.sasHello

//trait partIII
trait Logger{
  def log(message :String)
}

trait MyLogger extends Logger{

  //覆盖上层的抽象方法
  abstract override def log(message: String): Unit = {
    super.log(message)
  }
}

//trait partV
/**
  * 具体方法混合抽象方法，抽象方法在继承中实现
  */
trait Valid{
  def getName:String
  def valid : Boolean = {
    getName == "hello"
  }
}

class TValid(val getName:String) extends  Valid

/**
  * trait 继承类，这个类成为继承该trait 的父类
  */
trait vvvv extends  TValid

/**
  * 在Trait字段的初始化中，Trait是没有接收参数的构造函数，
  * 因此要求Trait对字段初始化时，需要使用Scala的高级特性：提前定义或使用Lazy Value
  */
trait SayHello{
  lazy val name:String = null
  println(name.toString)
}

class SayU(us :String) extends SayHello{
  override lazy val name:String = us
}

val ssss = new SayU("hello")
ssss.name

val testSeq = Seq(Array(0.0, 0.15, 0.5))
val retest = testSeq.flatten
println(retest)