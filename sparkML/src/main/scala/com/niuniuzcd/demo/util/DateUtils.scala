package com.niuniuzcd.demo.util

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import scala.collection.mutable.ArrayBuffer

/**
  * create by colin on 2018/7/16
  */
class DateUtils {
  lazy val cal: Calendar = Calendar.getInstance()


  val commDateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  final val DAY_DATE_FORMAT_ONE = "yyyy-MM-dd"
  final val SECOND_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss"
  final val DAY_DATE_FORMAT_TWO = "yyyyMMdd"
  final val DAY_DATE_FORMAT_THREE = "yyyy/MM/dd"
  final val DAY_DATE_FORMAT_FOUR = "yyyy-MM-dd HH"

  val f1 = new SimpleDateFormat(DAY_DATE_FORMAT_ONE)
  val f2 = new SimpleDateFormat(DAY_DATE_FORMAT_TWO)
  val f3 = new SimpleDateFormat(DAY_DATE_FORMAT_THREE)
  val f4 = new SimpleDateFormat(SECOND_DATE_FORMAT)
  val f5 = new SimpleDateFormat(DAY_DATE_FORMAT_FOUR)

  val dateFiled = Calendar.DAY_OF_MONTH

  //spark.sqlContext.udf.register("timeFormat", timeFormat)
  val timeFormat = (y: Any) => {
    val x = y.toString
    x.length match {
      case 8 if !x.contains("/") =>
        f1.format(f2.parse(x))
      case 9 if x.contains("/") =>
        f1.format(f3.parse(x))
      case 10 if x.contains("/") =>
        f1.format(f3.parse(x))
      case 10 if x.contains("-") =>
        f1.format(f1.parse(x))
      case 13 =>
        f1.format(f5.parse(x))
      case 19 =>
        f1.format(f4.parse(x))
      case _ if x.length > 19 =>
        f1.format(f4.parse(x))
      case _ =>
        x
    }
  }

  val timeFormat2 = (y: Any) => {
    val x = y.toString
    x.length match {
      case 8 if !x.contains("/") =>
        f1.parse(f1.format(f2.parse(x)))
      case 9 if x.contains("/") =>
        f1.parse(f1.format(f3.parse(x)))
      case 10 if x.contains("/") =>
        f1.parse(f1.format(f3.parse(x)))
      case 10 if x.contains("-") =>
        f1.parse(f1.format(f1.parse(x)))
      case 13 =>
        f1.parse(f1.format(f5.parse(x)))
      case 19 =>
        f1.parse(f1.format(f4.parse(x)))
      case _ if x.length > 19 =>
        f1.parse(f1.format(f4.parse(x)))
      case _ =>
        f1.parse(x)
    }
  }

  def getTimeRangeArray(begin: Date, endDate: Date, timeInter:Int = 1) = {
    cal.setTime(begin)
    var beginDate = begin
    val dateArray: ArrayBuffer[String] = ArrayBuffer()
    while (beginDate.compareTo(endDate) <= 0) {
      dateArray += f1.format(beginDate)
      cal.add(dateFiled, timeInter)
      beginDate = cal.getTime
    }
    dateArray :+ f1.format(endDate)
  }


  /**
    * get some days before
    *
    * @param days
    * @return
    */
  def getSomeDateAfter(inputStr: String, days: Int): String = {
    val startDate = f1.parse(inputStr)
    cal.setTime(startDate)
    cal.add(Calendar.DATE, days)
    f1.format(cal.getTime)
  }

  /**
    * get current days before
    *
    * @param days
    * @return
    */
  def getCurrentDateBefore(days: Int): String = {
    cal.setTime(new Date())
    cal.add(Calendar.DATE, -days)
    commDateFormat.format(cal.getTime)
  }

  /**
    * get some days before
    *
    * @param days
    * @return
    */
  def getSomeDateBefore(inputStr: String, days: Int): String = {
    val startDate = commDateFormat.parse(inputStr)
    cal.setTime(startDate)
    cal.add(Calendar.DATE, -days)
    commDateFormat.format(cal.getTime)
  }

  /**
    * 获取指定年份第一天
    *
    * @param year
    * @return
    */
  def getYearFirst(year: Int): String = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    cal.clear()
    cal.set(Calendar.YEAR, year)
    dateFormat.format(cal.getTime)
  }

  /**
    * 获取指定年份最后一天
    *
    * @param year
    * @return
    */
  def getYearLast(year: Int): String = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    cal.clear()
    cal.set(Calendar.YEAR, year)
    cal.roll(Calendar.DAY_OF_YEAR, -1)
    dateFormat.format(cal.getTime)
  }


  /**
    * 获取当前年份第一天
    *
    * @return
    */
  def getCurrentYearFirst: String = {
    val currentYear = cal.get(Calendar.YEAR)
    getYearFirst(currentYear)
  }

  /**
    * 获取当前年份最后一天
    *
    * @return
    */
  def getCurrentYearLast: String = {
    val currentYear = cal.get(Calendar.YEAR)
    getYearLast(currentYear)
  }

  /**
    * 获取当前时间
    *
    * @return
    */
  def getNowDate: String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    dateFormat.format(now)
  }

  def getNowDateYYMMdd: String = {
    lazy val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    dateFormat.format(now)
  }

  /**
    * 获取昨天日期
    *
    * @return
    */
  def getYesterday: String = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    cal.add(Calendar.DATE, -1)
    dateFormat.format(cal.getTime)
  }

  /**
    * 获取明天
    *
    * @return
    */
  def getTomorrowDay_YYYY_MM_DD_00_00_00: String = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    cal.setTime(new Date)
    cal.set(Calendar.HOUR_OF_DAY, 0)
    cal.set(Calendar.MINUTE, 0)
    cal.set(Calendar.SECOND, 0)
    cal.set(Calendar.MILLISECOND, 0)
    cal.add(Calendar.DATE, 1)
    dateFormat.format(cal.getTime)
  }

  /**
    * 获取当前周开始时间
    *
    * @return
    */
  def getCurrentWeekStartDay: String = {
    val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY)
    df.format(cal.getTime)
  }

  /**
    * 获取当前周最后时间
    *
    * @return
    */
  def getCurrentWeekDayEnd: String = {
    val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY); //这种输出的是上个星期周日的日期，因为老外把周日当成第一天
    cal.add(Calendar.WEEK_OF_YEAR, 1) // 增加一个星期，才是我们中国人的本周日的日期
    df.format(cal.getTime)
  }

  /**
    * 获取本月最后一天
    *
    * @return
    */
  def getCurrentMonthEnd: String = {
    val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    cal.set(Calendar.DATE, 1)
    cal.roll(Calendar.DATE, -1)
    df.format(cal.getTime)
  }
}

object  DateUtils extends  DateUtils
