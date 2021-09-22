package com.niuniuzcd.demo.test

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, callUDF, col, lit, max, row_number, when}
import org.apache.spark.sql.types.DoubleType

import scala.util.Try

/**
  * @author Cock-a-doodle-doo!
  */
object PreProcessTools {


  /**
    * 分为点数组
    */
  val PERCENTILE: Array[Double] = Array(0.25, 0.5, 0.75)
  val PERCENTILE_ALL: Array[Double] = Array(0d, 0.25, 0.5, 0.75, 1d)

  private val ROW_NUMBER_COL_NAME:String = "_rn"
  private val FEATURE_COL_NAME:String = "_feature"
  private val FEATURE_VALUE_NAME:String = "_value"
  private val CONVERT_TYPE:String = "double"


  /**
    * 使用开窗函数生成行号
    *
    * @param df               输入数据
    * @param orderByColName   order列名称
    * @param partitionColName 分区列名称
    * @param rowNumberColName 行号列名称
    * @return
    */
  def genRowNumber(df: DataFrame, orderByColName: String, partitionColName: String = "", rowNumberColName: String = ROW_NUMBER_COL_NAME): DataFrame = {
    if (partitionColName.nonEmpty) {
      df.withColumn(rowNumberColName, row_number()
        .over(Window.partitionBy(partitionColName).orderBy(orderByColName)).cast(CONVERT_TYPE))
    } else {
      df.withColumn(rowNumberColName, row_number()
        .over(Window.orderBy(orderByColName)).cast(CONVERT_TYPE))
    }
  }

  /**
    * 将行的列转换位double类型
    *
    * @param df        输入数据
    * @param colsArray 列数组
    * @return 转换后的数据
    */
  def rowColumns2Double(df: DataFrame, colsArray: Array[String]): Try[DataFrame] = {
    import org.apache.spark.sql.functions._
    Try(df.select(colsArray.map(f => col(f).cast(DoubleType)): _*))
  }

  /**
    * 列转行，应用与行转列计算后还原为之前的数据格式
    * 使用透视函数
    *
    * @param df             输入的数据
    * @param featureColName 特征列名称
    * @param rowNumber      数据列行号，类似主键，用于join的连接
    * @param value          特征值列
    * @param featureCols    特征列名取值（值为列名称）
    * @return 转换后的数据
    */
  def col2Row(df: DataFrame, featureCols: Array[String], featureColName: String = FEATURE_COL_NAME, rowNumber: String = ROW_NUMBER_COL_NAME, value: String = FEATURE_VALUE_NAME): DataFrame = {
    df.select(featureColName, rowNumber, value).groupBy(rowNumber).pivot(featureColName, featureCols).agg(
      max(value)
    )
  }


  /**
    * 行转列，字符串拼接
    *
    * @param columns 输入的列数组
    * @return 返回拼接好的字符串
    */
  def getStackParams(columns: String*): String = {
    val buffer = StringBuilder.newBuilder
    var size = 0
    size += columns.length
    buffer ++= s"stack($size "
    for (s <- columns) buffer ++= s",'$s', $s"
    buffer ++= ")"
    buffer.toString()
  }

  /**
    * 行转列，目的是多特征/字段并行计算
    *
    * @param df         输入的dataframe数据
    * @param columns    转换的列
    * @param primaryKey 主键/唯一标识数据行的键
    * @return 转换后的数据列
    */
  def row2Col(df: DataFrame, columns: Array[String], primaryKey: String = ROW_NUMBER_COL_NAME): DataFrame = {
    df.selectExpr(primaryKey, s"${getStackParams(columns: _*)} as ($FEATURE_COL_NAME, $FEATURE_VALUE_NAME)")
  }

  /**
    * 求分为点或者均值
    *
    * @param df             输入数据
    * @param groupByColName 分组列名称
    * @param valueColName   值列名称
    * @param avgFlag        是否求平均值标识
    * @return 新的数据集
    */
  def percentileApproxOrMean(df: DataFrame, avgFlag: Boolean, percentileArray: Array[Double] = PERCENTILE, groupByColName: String = FEATURE_COL_NAME, valueColName: String = FEATURE_VALUE_NAME): DataFrame = {
    if (avgFlag) {
      df.na.drop().groupBy(groupByColName).agg(
        callUDF("percentile_approx", col(valueColName), lit(percentileArray)).as("percentile"),
        avg(FEATURE_VALUE_NAME).as("avg") //需要删除 NaN参数
      )
    } else {
      df.groupBy(groupByColName).agg(
        callUDF("percentile_approx", col(valueColName), lit(percentileArray)).as("percentile") //无需NaN处理
      )
    }
  }


  /**
    * IQR = Q3 - Q1
    * lower = Q1  - 1.5 * IQR
    * upper = Q3 + 1.5 * IQR
    * 以分为点Array(0.25, 0.5, 0.75) 计算点，对应点数组下标为0，1，2
    * 如果以Array(0d, 0.25, 0.5, 0.75,1d) 计算分为点，那么 median下标为2，Q1下标为1，Q3为3，以此类推
    *
    * @param df 输入的数据集
    * @return 分为点数据集
    */
  def outlierDf(df: DataFrame, Q2: Int = 1, Q1: Int = 0, Q3: Int = 2, avgFlag: Boolean = false): DataFrame = {
    if (avgFlag) {
      //均值无需算中位点
      df.withColumn("lower", col("percentile").getItem(Q1) - (col("percentile").getItem(Q3) - col("percentile").getItem(Q1)) * 1.5)
        .withColumn("upper", col("percentile").getItem(Q3) + (col("percentile").getItem(Q3) - col("percentile").getItem(Q1)) * 1.5)
    } else {
      df.withColumn("median", col("percentile").getItem(Q2))
        .withColumn("lower", col("percentile").getItem(Q1) - (col("percentile").getItem(Q3) - col("percentile").getItem(Q1)) * 1.5)
        .withColumn("upper", col("percentile").getItem(Q3) + (col("percentile").getItem(Q3) - col("percentile").getItem(Q1)) * 1.5)
    }

  }

  /**
    * 缺失值填充
    *
    * @param df                输入数据集合
    * @param dropPercentileCol 是否删除分为点字段
    * @param avgFlag           是否使用均值填充
    * @return 填充的数据集合
    */

  def fillOutlierDf(df: DataFrame, dropPercentileCol: Boolean = true)(avgFlag: Boolean): DataFrame = {
    if (dropPercentileCol) {
      df.drop("percentile")
    }
    if (avgFlag) {
      df.withColumn(FEATURE_VALUE_NAME,
        when(col(FEATURE_VALUE_NAME) <= col("lower")
          || col(FEATURE_VALUE_NAME) >= col("upper"), lit(col("avg"))).otherwise(col(FEATURE_VALUE_NAME))) //使用均值补
    } else {
      df.withColumn(FEATURE_VALUE_NAME,
        when(col(FEATURE_VALUE_NAME) <= col("lower")
          || col(FEATURE_VALUE_NAME) >= col("upper"), lit(col("median"))).otherwise(col(FEATURE_VALUE_NAME))) //使用均值补
    }
  }

  /**
    * 查找数据集合中第一个为数字类型的字段
    *
    * @param df 输入数据集
    * @return 数字字段名称
    */
  def findFirstNumberCol(df: DataFrame): String = {
    val numericTypeString = Array("ByteType", "DecimalType", "DoubleType", "FloatType", "IntegerType", "LongType", "ShortType")
    df.dtypes.find(f => numericTypeString.contains(f._2)).get._1
  }
}
