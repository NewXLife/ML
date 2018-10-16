package com.niuniuzcd.demo.util

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/**
  * create by colin on 2018/7/16
  */
object DataUtils extends App {
  lazy val cal: Calendar = Calendar.getInstance()


  val commDateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

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
