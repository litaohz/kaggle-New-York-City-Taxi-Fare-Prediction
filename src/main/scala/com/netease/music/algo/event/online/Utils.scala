package com.netease.music.algo.event.online

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

object Utils {
  def getNowDate(): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val hehe = dateFormat.format(now)
    hehe
  }

  def getZeroTime(): Long = {
    val now: Date = new Date()
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd-hh-mm-ss")
    val a = dateFormat.parse(dateFormat.format(now)).getTime
    a
  }

  def test(): Long = {
    val cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.MINUTE, - 5)
    val minute = cal.get(Calendar.MINUTE)/5*5
    cal.set(Calendar.MINUTE, minute)
    cal.set(Calendar.SECOND, 0)
    cal.set(Calendar.MILLISECOND, 0)
    cal.getTimeInMillis
  }

  def main(args: Array[String]): Unit = {
    println(test())
  }
}
