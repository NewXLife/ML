package beginners_guide_scala

import scala.annotation.implicitNotFound
import scala.io.Source

/**
  * apply and unapply
  * apply 构建实例  unapply提取器（一般放到伴生对象）
  */
trait User {
  def name: String
}

class Auser(val name: String, val score: Int, val upgrade: Double) extends User
case class TTUser(name: String)
/**
  * 伴生对象中
  */
object Auser {
  def unapply(arg: Auser): Option[(String, Int, Double)] = Some((arg.name, arg.score, arg.upgrade))
}

class Buser(val name: String, val score: Int, val upgrade: Double) extends User
object Buser {
  def apply(name: String, score: Int, upgrade: Double) = new Buser(name, score, upgrade)
  def unapply(arg: Buser): Option[(String, Int, Double)] = Some((arg.name, arg.score, arg.upgrade))
}


object UnApplyTest extends App {
  val user: User = new Auser("test", 10, 10.0)
  user match {
    case Auser(n, _, _) => if (n.equals("test")) s"name:$n" else "hello, name mismatch"
    case Buser(n, _, _) => s"hello Buser $n"
  }

  /**模式匹配变量绑定**/
  val user1: TTUser = TTUser("hello")
  user1 match {
    case  a @ TTUser("hello") => println(a.name)
    case b @ TTUser("java") => println(b.name)
  }

  val xs = 3 :: 6 :: 12 :: 24 :: Nil
  xs match {
    case List(a, b, _*) => a * b
    case _ => 0
  }

  //
  if(xs.forall(_.isNaN)) None else Some(xs)

  /**
    * for语句中的模式匹配
    */
  val lists = List(1, 2, 3) :: List.empty :: List(5, 3) :: Nil
  val rest =  for {
    /**
      * 过滤非空列表
      */
    list @ head :: _ <- lists
  } yield list.size


  def gameResults(): Seq[(String, Int)] =
    ("Daniel", 3500) :: ("Melissa", 13000) :: ("John", 7000) :: Nil
  def hallOfFame = for {
    result <- gameResults()
    (name, score) = result
    if score > 5000
  } yield name


  /**
    * 函数式的责任链
    */

  /**
    * option
    * create option
    */
  //实例化some 样例类来创建option
  val greeting: Option[String] = Some("hello")
  val greetingNone: Option[String] = None

  //使用option工厂来创建
  val greetingFactory: Option[String] = Option(null)
  val greetingFactory2: Option[String] = Option("hello")

  //当值不存在时，使用默认值
  /**
    * 作为	getOrElse	参数的默认值是一个	传名参数	，
    * 这意味着，只有当这 个	Option	确实是	None	时，传名参数才会被求值。
    * 因此，没必要担心创建默 认值的代价，它只有在需要时才会发生。
    *
    * @param name
    * @param age
    * @param gender
    */
  case class TUser(name: String, age: Int, gender: Option[String])

  val tuser = TUser("test", 10, None)
  println(tuser.gender.getOrElse(""))


  //使用模式匹配处理Option
  //现用模式匹配处理Option实例是非常啰嗦的
  val gender = tuser.gender match {
    case Some(gender1) => gender1
    case None => "not define"
  }
  println(s"gender:$gender")

  // Option是类型A的容器，更确切地说，
  // 你可以把它看作是某 种集合，
  // 这个特殊的集合要么只包含一个元素，要么就什么元素都没有
  val tusers: Option[TUser] = Some(TUser("hello", 10, Some("male")))
  val tusersNone: Option[TUser] = None

  //  val st = Map(1 -> TUser("hello", 10, Some("male")))
  //  val stt: Option[TUser] = st.get(1)

  // 虽然在类型层次上，Option	并不是	Scala	的集合类型，
  // 但凡是你觉得	Scala 集合好用的方法，Option	也有，
  // 你甚至可以将其转换成一个集合，比如说	List  这个作用可以让你执行一个副作用，比如foreach方法
  // 如果这个	Option	是一个	Some，
  // 传递给		foreach	的函数就会被调用一次，且 只有一次；
  tusers.foreach(user => println(user.age))

  // 如果是		None	，那它就不会被调用
  tusersNone.foreach(user => println(user.age))

  //执行映射
  val tage = tusers.map(_.age)
  println(s"option execute map:$tage")

  //option or flatMap
  val tgender = tusers.map(_.gender)
  println(tgender) // Option[Option[String]]
  //使用flatMap
  val tfgender = tusers.flatMap(_.gender)
  println(tfgender) // Option[String]

  // Option   for
  val st = for {
    tuser <- tusers
  } yield tuser.age
  //  st.foreach(x => println(x))

  // 在生成器左侧使用
  for {
    TUser(age, _, _) <- tusers
  } yield age


  /**
    * scala error handler
    */
  //常规的异常处理
  case class Tiger(name: String)

  class Tiger2

  case class UnderAgeException(msg: String) extends Exception(msg)

  def buyTiger(tiger: Tiger) = {
    if (tiger.name.equals("test"))
      throw UnderAgeException("test")
    else new Tiger2
  }

  val testtiger = Tiger("hello")
  try {
    buyTiger(testtiger)
  } catch {
    case UnderAgeException(msg) => msg
  }

  // 函数式的错误处理 Try
  // Option[A]	是一个可能有值也可能没值的容器，
  // Try[A]	则表示一种计算：这种计算在成功的情况下返回类型为A的值，在出错的情况下，返回	Throwable
  // Try	有两个子类型：
  // 1.Success[A]	：代表成功的计算。
  // 2.	封装了Throwable	的Failure[A]：代表出了错的计算
  import scala.util.Try
  import java.net.URL

  def parseURL(url: String): Try[URL] = Try(new URL(url))

  // 给定的	url	语法正确，这将是Success[URL]，
  // 否则，URL构造器会引发MalformedURLException，从而返回值变成Failure[URL]类型
  val url = parseURL("www.baidu.com").getOrElse(new URL("https://www.aliyun.com"))

  //Try 的链式操作
  parseURL("").map(_.getProtocol)
  //parseURL(scala.io.StdIn("URL: ")).map(_.getProtocol)

  //flatMap / foreach / filter
  parseURL("").flatMap { u =>
    Try(u.openConnection()) flatMap (conn => Try(conn.getInputStream))
  }

  // for 语句中的 Try
  def getUrlContext(url: String): Try[Iterator[String]] = for {
    url <- parseURL(url)
    source = Source.fromURL(url)
  } yield source.getLines()

  // 使用模式匹配
  import scala.util.Success
  import scala.util.Failure

  getUrlContext("test") match {
    case Success(context) => context.foreach(println(_))
    case Failure(ex) => println(s"error:${ex.getMessage}")
  }

  // 使用recover 从故障中恢复
  // 	recover	，它接受一个偏函数，并返回另一个	Try。
  // 如果	recover	是 在	Success	实例上调用的，那么就直接返回这个实例，否则就调用偏函数。
  // 如果 偏函数为给定的	Failure	定义了处理动作，recover	会返回Success，里 面包含偏函数运行得出的结果。
  import java.net.MalformedURLException
  import java.io.FileNotFoundException

  getUrlContext("test") recover {
    case e: FileNotFoundException => Iterator("file not found exception")
    case ex: MalformedURLException => Iterator(s"${ex.getMessage}")
    case _ => Iterator("unexpected error")
  }

  // Try orElse / transform / recoverWith


  /**
    * Either
    * 一种可以代表计算的类型，但它的可使用范围要 比	Try	大的多。
    * Try	不能完全替代Either，它只是	Either	用来处理异常的一个特殊用法。
    * Try	和Either	互相补充，各自侧重于不同的使用场景
    */

  // Either	也是一个容器类型，但不同于	Try、Option，它需要两个类型参数：
  // Either[A,	B]要么包含一个类型为 A	的实例，要么包含一个类型为 B 的实例
  // Either	只有两个子类型：	Left、	Right，
  // 如果	Either[A,	B]对象包含的是 A	的实例，那它就是 Left 实例，否则就是	Right	实例。

  // 使用方法 isLeft / isRight
  val eightertest: Either[String, String] = Left("left")
  val eightertest1: Either[String, String] = Right("right")
  println(eightertest.isLeft)
  println(eightertest1.isLeft)

  def getEightertest(str: String): Either[String, String] =
    if (str.contains("hello"))
      Left("left hello")
    else
      Right("right hello")

  // 惯用使用方法
  getEightertest("test") match {
    case Left(msg) => println(msg)
    case Right(msg) => println(msg)
  }

  // 你不能，至少不能直接像Option、Try	那样把Either当作一个集合来使用，因为 Either	是	无偏(unbiased)	的。
  // Try偏向Success, map、flatMap以及其他一些方法都假设	Try	对象是一个Success实例，如果是	Failure，那这些方法不做任何事情，直接将这个	Failure返回

  // 但	Either	不做任何假设，这意味着首先你要选择一个立场，
  // 假设它是	Left	还是 Right，然后在这个假设的前提下拿它去做你想做的事情。
  // 	调用	left	或 right	方法，就能得到	Either	的LeftProjection或RightProjection	实 例，
  // 这就是	Either的立场(Projection)，它们是对	Either	的一个左偏向的或右偏向 的封装。

  //Eigher 使用场景
  // 1. 错误处理

  //可以用	Either	来处理异常，就像	Try	一样。不过	Either	有一个优势：
  // 可以使用更 为具体的错误类型，而Try	只能用Throwable	。
  // （这表明	Either	在处理自定义 的错误时是个不错的选择）
  // 不过，需要实现一个方法，将这个功能委托给	scala.util.control包中的Exception对象

  import java.net.MalformedURLException
  import scala.util.control.Exception.catching

  def handing[Ex <: Throwable, T](exType: Class[Ex])(b: => T): Either[Ex, T] = catching(exType).either(b).asInstanceOf[Either[Ex, T]]

  def parseTestUrl(str: String): Either[MalformedURLException, URL] = {
    handing(classOf[MalformedURLException])(new URL(""))
  }

  // 当按顺序依次处理一个集合时，里面的某个元素产生了意料之外的结果，
  // 但是这时程序不应该直接引发异常，因为这样会使得剩下的元素无法处理。Either也非常适用于这种情况。


  //高阶函数function
  case class Email(subject: String, text: String, sender: String, recipient: String)

  //类型别名的使用使参数更有字面的意义
  type EmailFilter = Email => Boolean

  //define a email filter
  def newMails(mails: Seq[Email], f: EmailFilter) = mails.filter(f)

  val sendByOneOf: Set[String] => EmailFilter =
    senders =>
      email => senders.contains(email.sender)

  val notSentByAnyOf: Set[String] => EmailFilter =
    senders =>
      email => !senders.contains(email.sender)

  val minimumSize: Int => EmailFilter =
    n =>
      email => email.text.length >= n

  val maxmumSize: Int => EmailFilter =
    n =>
      email => email.text.length <= n

  val emilFilter: EmailFilter = notSentByAnyOf(Set("tiger@example.com"))
  val mails = Email("", "", "", "") :: Nil

  newMails(mails, emilFilter)

  //  val tt : Int => Int => Int =
  //    t =>
  //      s => t + s

  // 重用已有的函数
  type SizeChecker = Int => Boolean
  val sizeConstraint: SizeChecker => EmailFilter =
    f =>
      email => f(email.text.length)

  val minimumSize2: Int => EmailFilter =
    n =>
      sizeConstraint(_ >= n)

  val maxmumSize2: Int => EmailFilter =
    n =>
      sizeConstraint(_ <= n)

  /**
    * 函数组合 compose andThen pipeline
    */

  // 给定一个类型为	A	=>	Boolean		的谓词，它返回一个新函数，这个新函数总是得出和谓词相对立的结果：
  def complement[A](predicate: A => Boolean) = (a: A) => !predicate(a)

  //andThen
  //这个函数首先应用sentByOneOf	到参数	Set[String]	上，产生一个EmailFilter谓词,然后应用complement到这个谓词上
  val notSentByAnyOfNew = sendByOneOf andThen (g => complement(g))
  val notSentByAnyOfNew1 = sendByOneOf andThen (complement(_))

  //compose
  //f.compose(g)返回一个新函数，调用这个新函数时，会首先调用g，然后应用f	到g	的返回结果上
  val notSentByAnyOfNew3 = complement _ compose sendByOneOf

  def f(s:String) = s"f function($s)"
  def g(s:String) = s"g function($s)"
  // f(g(x)) f function(g function(hello))
  val composefg =  f _ compose g
  println(composefg("hello"))

  // g(f(x))  g function(f function(hello))
  val andthenfg = f _ andThen g
  println(andthenfg("hello"))

  /**
    * 设置多个过滤器
    * @param predicates
    * @tparam A
    * @return
    */
  //any	函数返回的新函数会检查是否有一个谓词对于输入a	成真
  def any[A](predicates: (A => Boolean)*): A => Boolean =
    a => predicates.exists(pred => pred(a))

  //	none返回的是any返回函数的补，只要存在一个成真的谓词，none	的条件就无法满足
  def none[A](predicates: (A => Boolean)*) = complement(any(predicates: _*))

  // 	every	利用none和any来判定是否每个谓词的补对于输入	a	都不成真
  def every[A](predicates: (A => Boolean)*) = none(predicates.view.map(complement(_)): _*)

  val filter: EmailFilter = every(
    notSentByAnyOf(Set("")),
    minimumSize2(100),
    maxmumSize(200)
  )

  //pipeline
  val addMisssubject = (email : Email) =>
    if (email.subject.isEmpty) email.copy(subject = "hello") else email

  val checkSpelling = (email :Email) =>
    email.copy(text = email.text.replaceAll("your", "you're"))



  val pipeline = Function.chain(Seq(addMisssubject, checkSpelling))

  /**
    * 部分函数应用(函数重用的机制)
    */
  type IntPairPred = (Int, Int) => Boolean
  def sizeConstraint2(pred: IntPairPred, n: Int, email: Email) =
    pred(email.text.length, n)

  val gt: IntPairPred = _ > _
  val ge: IntPairPred = _ >= _
  val lt: IntPairPred = _ < _
  val le: IntPairPred = _ <= _
  val eq: IntPairPred = _ == _

  //对所有没有传入值的参数，必须使用占位符_	还需要指定这些参数的类型，这使得函数的部分应用多少有些繁琐。
  val minxxx: (Int, Email) => Boolean = sizeConstraint2(gt, _:Int, _: Email)

  // 不过，你可以绑定或漏掉任意个、任意位置的参数。比如，我们可以漏掉第一个值，只传递约束值n
  val minxxx2:(IntPairPred, Email) => Boolean = sizeConstraint2(_ : IntPairPred, 20, _ :Email)

  // 从方法到函数对象
  val sizexxxx: (IntPairPred, Int, Email) => Boolean = sizeConstraint2


  /**
    * 柯里化
    */
  //	sizeConstraint3	接受一个IntPairPred 返回一个函数，
  // 这个函数又接受Int	类型的参数返回另一个函数，最终的这个函数接受一个Email，返回布尔值
  def sizeConstraint3(pred: IntPairPred)(n:Int)(email: Email):Boolean =
    pred(email.text.length, n)

  val minSize: Int => Email => Boolean = sizeConstraint3(ge)
  val minSize20 : Email => Boolean = minSize(20)
  val minSize20_version2: Email => Boolean  = sizeConstraint3(ge)(20)

  val sum:(Int, Int) => Int  = _ + _
  val sumCurried : Int => Int => Int = sum.curried
  val sumUnCurried = Function.uncurried(sumCurried)

  /**
    * 函数化的依赖注入
    */
  trait EmailRepository{
    def geMails(user: User, unread: Boolean) : Seq[Email]
  }
  trait FilterRepository{
    def getEmailFilter(user: User): EmailFilter
  }
  trait MailBoxServer{
    def getNewMails(emailRepository: EmailRepository)(filterRepository: FilterRepository)(user: User) =
      emailRepository.geMails(user, true).filter(filterRepository.getEmailFilter(user))
    val newMails : User => Seq[Email]
  }

  object MockEmailRep extends EmailRepository{
    def geMails(user: User, unread: Boolean): Seq[Email] = Nil
  }

  object MocckFilter extends FilterRepository{
    override def getEmailFilter(user: User): EmailFilter = _ => true
  }

  object MockServer extends MailBoxServer{
    override val newMails: User => Seq[Email] = getNewMails(MockEmailRep)(MocckFilter)
  }

  //--------------------stage2-------------------------------
  /**
    * 类型类
    */

  //step 1, create trait
  object Math {

    // 类型类NumberLike定义一些行为
    //一个类型类C	定义了一些行为，要想成为C	的一员，类型T	必须支持这些 行为。
    // 一个类型	T	到底是不是	类型类C	的成员，这一点并不是与生俱来的。
    // 开发者可以实现类必须支持的行为，使得这个类变成类型类的成员。
    // 	一旦T	变 成	类型类C	的一员，参数类型为类型类C成员的函数就可以接受类型	T	的 实例。
    @implicitNotFound("no member of type class NumberLike in scope for ${T}")
    trait NumberLike[T] {
      def plus(x: T, y: T): T
      def divide(x: T, y: Int): T
      def minus(x: T, y: T): T
    }

    // 提供默认成员
    // step2 是在伴生对象里提供一些默认的类型类特质实现
    object NumberLike {

      //类型类的成员通常是单例对象
      implicit object NumberLikeDouble extends NumberLike[Double] {
        def plus(x: Double, y: Double): Double = x + y

        def divide(x: Double, y: Int): Double = x / y

        def minus(x: Double, y: Double): Double = x - y
      }

      //类型类的成员通常是单例对象，implicit 它让类型类成员隐式可用
      implicit object NumberLikeInt extends NumberLike[Int] {
        def plus(x: Int, y: Int): Int = x + y

        def divide(x: Int, y: Int): Int = x / y

        def minus(x: Int, y: Int): Int = x - y
      }

    }

  }

  //将参数限制在特定类型类的成员上，是通过第二个implicit参数列表实现的。
  // 这是什么意思？这是说，当前作用域中必须存在一个隐式可用的NumberLike[T]对象，
  // 比如说，当前作用域声明了一个隐式值(implicit	value)。这种声明很多时候都是通过导入一个有隐式值定义的包或者对象来实现的。
  // 当且仅当没有发现其他隐式值时，编译器会在隐式参数类型的伴生对象中寻找。
  // 作为库的设计者，将默认的类型类实现放在伴生对象里意味着库的使用者可以轻易的重写默认实现，
  // 这正是库设计者喜闻乐见的。	用户还可以为隐式参数传递一个显示值，来重写作用域内的隐式值
  object Statistics2 {

    import Math.NumberLike

    //方法带有一个类型参数T，接受类型为Vector[T]的参数, ev为隐式转换参数
    def mean[T](xs: Vector[T])(implicit ev: NumberLike[T]): T =
      ev.divide(xs.reduce(ev.plus(_, _)), xs.size)

    //上下文绑定
    //总是带着这个隐式参数列表显得有些冗长。
    // 对于只有一个类型参数的隐式参数，Scala提供了一种叫做上下文绑定(context	bound)的简写
    //如果类型类型类需要多个类型参数，就不能使用上下文绑定语法了
    // 上下文绑定	T:	NumberLike意思是，必须有一个类型为NumberLike[T]	的隐式值在当前上下文中可用，这和隐式参数列表是等价的
    //  def median2[T](xs: Vector[T])(implicit ev: NumberLike[T]) = xs(xs.size / 2)
    def median[T: NumberLike](xs: Vector[T]) =
      xs(xs.size / 2)

    def quartiles[T: NumberLike](xs: Vector[T]): (T, T, T) =
      (xs(xs.size / 4), median(xs), xs(xs.size / 4 * 3))

    // 如果想要访问这个隐式值，需要调用implicitly方法
    def iqr[T: NumberLike](xs: Vector[T]) =
      quartiles(xs) match {
        case (lower, _ , upper) =>
          implicitly[NumberLike[T]].minus(upper, lower)
      }
  }

  object Statistics {
    def median(v: Vector[Double]): Double = v(v.length / 2)

    def quartiles(xs: Vector[Double]): (Double, Double, Double) =
      (xs(xs.size / 4), median(xs), xs(xs.size / 4 * 3))

    def mean(xs: Vector[Double]): Double =
      xs.sum / xs.size
  }

  val classVtest = Vector[Double](2.0, 3.0, 4.0, 1.0)
  println("v.size", classVtest.size)
  println("v.length", classVtest.length)
  println("v.length/2", classVtest.length / 2)
  println(classVtest(1))
  println("median:", Statistics.median(classVtest))

  println(Statistics2.mean(classVtest))

  //	Vector[String]，你会在编译期得到一个错误，这个错误指出参数	ev:	NumberLike[String]没有隐式值可用。
  // 如果你不喜欢这个错误消息你可以用 @implicitNotFound 为类型类添加批注
  //  val classVtest2 = Vector[String]("hello","test")
  //  println(Statistics2.mean(classVtest2))

  //将自己的类型加入到类型类成员当中
object JodaImplicits{
  import Math.NumberLike
  import org.joda.time.Duration

    // 只需创建NumberLike	的一个隐式实现
  implicit object NumberLikeDuration extends  NumberLike[Duration]{
    def plus(x: Duration, y: Duration): Duration = x.plus(y)

    def divide(x: Duration, y: Int): Duration = Duration.millis(x.getMillis / y)

    def minus(x: Duration, y: Duration): Duration = x.minus(y)
  }
}

  /**
    * 路径依赖类型
    */

  object KongFu{
    case class Character(name: String)
  }
  class KongFu(name:String){
    import KongFu.Character
    def createKongFuActor(actor1: Character,actor2: Character ):(Character, Character) = (actor1, actor2)
  }

  val tianlongbabu = new KongFu("tianlongbabu")
  val shediaoyingxiong = new KongFu("shediaoyingxiong")

  val qiaofeng = KongFu.Character("qiaofeng")
  val guojin = KongFu.Character("guojin")

  //problem has happened, 天龙八部中出现了乔峰和郭靖
  tianlongbabu.createKongFuActor(qiaofeng, guojin)

  //问题解决1, 角色绑定一个系列， 错误发生在运行期，运行期不匹配会发生错误
  object KongFu1{
    case class Character(name: String, kongFu: KongFu1)
  }

  class KongFu1{
    import KongFu1.Character
    def createKongFu1(actor1: Character, actor:Character):(Character, Character) = {
      //创建的时候做检查
      require(actor.kongFu == actor1.kongFu)
      (actor1, actor)
    }
  }

  //更加快速的失败让它在编译期失败，建立Character和KongFu 之间的联系 编码在类型层面上
  // Actor 嵌套在Movie， 角色依赖于具体的电影实例
  class Movie{
    class Actor
    var b: Option[Actor] = None
  }

  val tomb_raider = new Movie
  val The_Lord_of_the_Rings = new Movie

  val Angelina_Jolie  = new tomb_raider.Actor
  val wangzi  = new The_Lord_of_the_Rings.Actor

  //你只能将特定角色放到特定的电影里面
  tomb_raider.b = Some(Angelina_Jolie)
  The_Lord_of_the_Rings.b = Some(wangzi)

  //你不能将安吉丽娜·朱莉放到指环王电影里
  //zhihuanwang.b = Some(anjielinazhuli)


  class KongFu2(name: String){
    case class Character(actorName: String)
    def createKongFuActor(a1: Character, a2: Character):(Character, Character) = (a1, a2)
  }

  /**
    * 依赖方法类型
    */
  // 被依赖的实例只能在一个单独的参数列表里
  // 如果这个方法不在这个类定义，可以使用依赖方法类型，参数的类型信息依赖前面的参数
  class KongFu3(name:String){
    def createKongFuAcotr(f:KongFu2)(a1: f.Character, a2: f.Character): (f.Character, f.Character) = (a1, a2)
  }

  val Kongfu3test = new KongFu3("test")
  val kongfu2test = new KongFu2("test2")

  val k2Actor1 = kongfu2test.Character("a1")
  val k2Actor2 = kongfu2test.Character("a2")

  val (k2a1, k2a2) = Kongfu3test.createKongFuAcotr(kongfu2test)(k2Actor1, k2Actor2)
  println("kongfu2:",k2a1, k2a2)


  /**
    * 依赖方法类型通常和抽象类型成员一起使用
    */
  //抽象类型成员
  object DBserver{
     abstract class DB(val name: String){
       type Value
     }
  }
  import DBserver.DB
  class CommonDBServer{
    val data = Map.empty[DB, Any]
    def get(db: DB): Option[db.Value] =data.get(db).asInstanceOf[Option[db.Value]]

    def set(db: DB)(value: db.Value)= data.updated(db, value)
  }

  trait HiveDb extends DB{
    type Value = Int
  }

  trait MysqlDb extends  DB{
    type Value = Double
  }

  object DBs {
    val hiveDb = new DB("hiveDb") with HiveDb
    val mysqlDb = new DB("mysql") with MysqlDb
  }

  val dataSore = new CommonDBServer
  dataSore.set(DBs.hiveDb)(23)
  dataSore.set(DBs.mysqlDb)(23)
  dataSore.get(DBs.mysqlDb)


  /**
    * 自身类型
    */
  trait Logged{
    def getMsg():String = "hello"
  }

  class LoggerTest  {
    //自身类型表明在实例化loggerTest 必须混入带getMsg方法的（或者是某个类型）
    this: Logged =>
    def log:String =  "hello"
  }

  class S3 extends LoggerTest with Logged{

  }

}






