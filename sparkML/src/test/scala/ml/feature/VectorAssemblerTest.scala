package ml.feature

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DoubleType
import util.SparkTools

/**
  * 字段转换成特征向量
  */
object VectorAssemblerTest extends SparkTools {

  import org.apache.spark.sql.functions._
  import spark.implicits._

  baseDf.show(5, truncate = 0)
  baseDf.printSchema()
  /**
    * root
    * |-- d14: string (nullable = true)
    * |-- ad: string (nullable = true)
    * |-- day7: string (nullable = true)
    * |-- m1: string (nullable = true)
    * |-- m3: string (nullable = true)
    * |-- m6: string (nullable = true)
    * |-- m12: string (nullable = true)
    * |-- m18: string (nullable = true)
    * |-- m24: string (nullable = true)
    * |-- m60: string (nullable = true)
    */

  //get all columns return Array[String]
  val colsName = baseDf.columns.toBuffer


  //删除日期字符串类型（这个类型不能转换为数字类型）
  colsName.remove(colsName.indexOf("ad"))

  // transform data type
  val colsArray = colsName.map(f => col(f).cast(DoubleType))

  // 字段转换成特征向量
  val assembler = new VectorAssembler().setInputCols(colsName.toArray).setOutputCol("features")

  //transfer 2 double type
//  val str = Array("ad", "d14", "m1", "m3", "m6", "m12", "m18", "m24", "m60")
  //input parameters  String*
//  baseDf.selectExpr(str:_*).show(5, truncate =0)

  //cols: Column*
  val resDf = baseDf.select($"ad" +: colsArray: _*)

  //字符串类型需要转换为数字类型
  val vecDF = assembler.transform(resDf)

  //val dd = baseDf.select($"ad", $"d14".cast(DoubleType), $"m1".cast(DoubleType), $"m3".cast(DoubleType), $"m6".cast(DoubleType), $"m12".cast(DoubleType), $"m18".cast(DoubleType), $"m24".cast(DoubleType), $"m60".cast(DoubleType),$"day7".cast(DoubleType))
  //  val vecDF2 = assembler.transform(dd)

  vecDF.show(5, truncate = false)


  def String2DoubleType(df: DataFrame, fields: Array[String]): DataFrame = {
    import org.apache.spark.sql.functions._
    df.select(fields.map(f => col(f).cast(DoubleType)): _*)
  }

}
