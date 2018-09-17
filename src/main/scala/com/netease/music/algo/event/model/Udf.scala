package com.netease.music.algo.event.model

import java.util.Calendar

import org.apache.spark.sql.functions.udf

object Udf {

  def dealLabel = udf((label: Long) => {
   var res = 0
    if(label > 0){
      res = 1
    }
    res
  })

  def getNWeekDay: Int = {
    val calendar = Calendar.getInstance
    calendar.get(Calendar.DAY_OF_WEEK)
  }
  def testUserid = udf((userid: Long) => {
   userid % 10 == getNWeekDay
  })

  def testUserid(mod : Int) = udf((userid: Long) => {
    userid % mod == getNWeekDay
  })
  def main(args: Array[String]): Unit = {
    println(123213121212L%7 == getNWeekDay)
  }
}
