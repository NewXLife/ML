object MatchCaseTest extends App {
  def gameResults(): Seq[(String, Int)] = ("t1", 3) :: ("t2", 4) :: ("t3", 100) :: Nil

  def rrrrr = for {
    (c, score) <- gameResults()
    if score > 3
  } yield c
  println(rrrrr)

  /**
    * 模式匹配 @ 绑定变量
    */
  val l = List(2, 2, 3)
  val s = l match {
    // 匹配以List 开头为1 的list ，匹配到的放到变量list中
    case list@List(1, _*) => s"a start value is 1 list :$list"
    case list: List[_] => s"a start value not 1 list"
  }

  val lists = List(1, 2, 3) :: List.empty :: List(5, 3) :: Nil
  //list绑定到模式  head :: _ 上，如果匹配则 绑定变量list
  val t = for (list@head :: _ <- lists) yield list.size
  //head 表示list 的头 上面的模式匹配表达 匹配list非空的列表，并将列表的长度记录，最后生成所有非空list的长度 list
  println(t)


  def cc(arr: Any) = {
    arr match {
      case arr@"hello" => arr
      case arr@"java" => arr
      case arr@"scala" => arr
      case _ => "somethingelse"
    }
  }

  /**
    * 编译器无法从匿名函数中推导出类型，因此最好显示声明
    * 统计单词数量在5-10 的单词
    * 模式匹配的匿名函数
    */
  val words = ("h", 3) :: ("i", 6) :: ("c", 11) :: ("d", 15) :: Nil
  def wordsWithoutOutlines(wordFrequencies: Seq[(String, Int)]): Seq[String] = {
    wordFrequencies.filter{case (_, f) => f > 5 && f < 10}.map{case (w, _) => w}
  }

  val rwf = wordsWithoutOutlines(words)
  println(rwf)

  //上面的拆分
  type PC = (String, Int) => Boolean
  val predicate: PC = {
    case (_, f) => f > 5 && f < 10
  }

  val transform: (String, Int) => String = {
    case (w, _) => w
  }


  /**
    * 使用偏函数，更加简洁的方式
    */
  val pd: PartialFunction[(String,Int),String] = {
    case (w, f) if f > 5 && f < 10 => w
  }

  val pd1: (String, Int) => String = {
    case (w, f) if f > 5 && f < 10  => w
  }
  val rrr = words.collect(pd)
  println(rrr)

  println(pd1("hello", 10))
}
