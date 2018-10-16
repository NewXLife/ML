import util.SparkTools

/**
  * create by colin on 2018/8/23
  */
object  TestPolicy extends  SparkTools {
//  val df = loadCSVData("csv","/Users/colin/Desktop/bd_ML/docs/policy-test-dataset2.csv")
  val df = loadCSVData("csv","/Users/colin/Desktop/bd_ML/docs/policy-test-dataset2.csv")

//  df.show(100)
//
//  df.show(false)
//
//  df.isLocal

  spark.sparkContext.setLogLevel("ERROR")
  df.show(10)

  val conList = new java.util.ArrayList[SchemaItem]
  val t = new SchemaItem()
  t.setName("7day")
  t.setDesc("test")
  t.setPartition(true)
  t.setType("int")

  val t1= new SchemaItem()
  t1.setName("1month")
  t1.setDesc("test")
  t1.setPartition(true)
  t1.setType("int")

  val t3= new SchemaItem()
  t3.setName("3month")
  t3.setDesc("test")
  t3.setPartition(true)
  t3.setType("int")

  val t6= new SchemaItem()
  t6.setName("6month")
  t6.setDesc("test")
  t6.setPartition(true)
  t6.setType("int")

  val t12= new SchemaItem()
  t12.setName("12month")
  t12.setDesc("test")
  t12.setPartition(true)
  t12.setType("int")

  val t18= new SchemaItem()
  t18.setName("18month")
  t18.setDesc("test")
  t18.setPartition(true)
  t18.setType("int")

  val t24= new SchemaItem()
  t24.setName("24month")
  t24.setDesc("test")
  t24.setPartition(true)
  t24.setType("int")


  val t60= new SchemaItem()
  t60.setName("60month")
  t60.setDesc("test")
  t60.setPartition(true)
  t60.setType("int")


  val lebel= new SchemaItem()
  lebel.setName("d14")
  lebel.setDesc("test")
  lebel.setPartition(true)
  lebel.setType("int")

  conList.add(t1)
//  conList.add(t3)
//  conList.add(t6)
//  conList.add(t12)
//  conList.add(t18)
//  conList.add(t24)
//  conList.add(t60)

  val res = conList


//  StatisticCenter.apply.execCon(df, conList.toList, 111, 2222, lebel)


//  val catalog = ContinueEncoder.apply.getCatalogDouble(df, "7day")
//  catalog.foreach(println(_))
//  //return Array(bins, bins_min, bins_max, woe, iv)
//  val resArray = WOEIVEncoder.apply().woeIv(df, "7day", catalog, "d14")
//  resArray
//  assert( 1==1)
}
