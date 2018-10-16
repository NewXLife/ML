

object TOption extends App {
//  val greeting : Option[String] = Some("hello")
//  val g1 :Option[String] =None
//
//  val url = parseURL("").getOrElse(new URL(""))
//
//
//  def parseURL(url :String) :Try[URL] = Try(new URL(url))


  /**
    * 高阶函数应用
    * @param subject
    * @param text
    * @param sender
    * @param recipient
    */
  case class Email(subject:String,
             text:String,
              sender:String,
              recipient:String
             )

  type SizeChecker = Int => Boolean

  val sizechecker: SizeChecker => EmailFilter =
    f =>
      email => f(email.text.length)

  type EmailFilter = Email => Boolean
  def newMail(mails :Seq[Email], f:EmailFilter) = mails.filter(f)

  //产生EmailFilter的工厂方法
  val sentByOneOf : Set[String] => EmailFilter =
    senders =>
      email => senders.contains(email.sender)

  val notSendByOnOf :Set[String] => EmailFilter =
    senders =>
      email => !senders.contains(email.sender)

  val mini :Int => EmailFilter =
    n =>
      sizechecker(_ <= n)
//      email => email.text.length <= n

  val maxm :Int => EmailFilter =
    n =>
      sizechecker(_ >=n)
//      email => email.text.length >= n



  //create filter function
  val emailFilter: EmailFilter = sentByOneOf(Set("zcdhotmail@163.com"))

  val mails = Email(
    subject = "hello",
    text  = "hello",
    sender = "zcdhotmail@163.com",
    recipient =  "hello"
  ):: Email(
    subject = "hello",
    text  = "hello",
    sender = "ttt@163.com",
    recipient =  "hello"
  )::Email(
    subject = "hello",
    text  = "hello",
    sender = "res.com",
    recipient =  "hello"
  ):: Nil

  val res = newMail(mails, emailFilter)
  println("res----:",res)


  def complement[A](pre: A => Boolean) =
    (a:A) => !pre(a)



  //combine function f.andThen(g)返回的新函数执行流程 f的返回结果，再应用到g上
//  val notSendByAnyof = sentByOneOf andThen (complement(_))
  val notSendByAnyof = sentByOneOf andThen (g => complement(g))



  val notEmailFilter = notSendByAnyof(Set("zcdhotmail@163.com","ttt@163.com","test@163.com"))
  val res2 = newMail(mails, notEmailFilter)
  println("res2----:",res2)


  //compose f.compose(g) 返回到新函数，先调用g，再将结果给f执行
//  val comBine = sentByOneOf.compose(myCompose(_))


  val doubleNum :Int => Int = {
    num => num * 2
  }

  val addOne : Int => Int ={
    num => num + 1
  }

    //Set[String] -> EmailFilter
  def  myCompose2[A,B](g: A => B)  ={
    x => g(x)
  }



  def myCompose[A,B,C](g:A => B, f: C => A) : C=> B =
    x => g(f(x))

  val myc = myCompose(addOne, doubleNum)
  val myc1 = addOne compose doubleNum

}
