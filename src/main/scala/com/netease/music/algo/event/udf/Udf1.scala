package com.netease.music.algo.event.udf

import java.util.{Calendar, Collections}
import com.netease.music.algo.event.udf.Utils._

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf


object Udf1 {

  def concatUserid(str: String): UserDefinedFunction = udf((userid: String) => {
    userid.concat("\t").concat(str)
  })


  def splitVideos: UserDefinedFunction = udf((events: String) => {
    events.split(",")
  })

  def splitResult: UserDefinedFunction = udf((result: String) => {
    val res = result.split(",")
    if (res.size > 50)
      res.dropRight(res.length - 50)
    res
  })


  def splitResultWithNumber(number: Int): UserDefinedFunction = udf((result: String) => {
    val res = result.split(",")
    if (res.size > number) {
      val value = res.dropRight(res.length - number)
      value
    }
    else
      res
  })


  def splitResultAll: UserDefinedFunction = udf((result: String) => {
    result.split(",")
  })


  def position: UserDefinedFunction = udf((result: String) => {
    var pos = 1000
    result.split(",").map(x => {
      x.split(":")(1)
    }).indexOf("0")
  })


  def extractResult1(idPos: Int): UserDefinedFunction = udf((result: String) => {
    result.split(":")(idPos).split("-")(0)

  })


  def extractResult(idPos: Int): UserDefinedFunction = udf((result: String) => {
    result.split(":")(idPos)
  })


  def value: UserDefinedFunction = udf((value: Seq[Double]) => {
    value(1)
  })


  def splitVideos1: UserDefinedFunction = udf((events: String) => {
    var res = -1L
    try {
      res = events.split(",")(0).split(":")(0).toLong
    }
    catch {
      case e: NumberFormatException =>
    }
    res
  })

  def splitVideosWIthConstrain(constrain: Integer): UserDefinedFunction = udf((events: String) => {
    val videos = events.split(",")
    videos.drop(videos.length / constrain)
  })


  def extractVideoId(idPos: Int): UserDefinedFunction = udf((videoInfo: String) => {
    var res = -1L
    try {
      res = videoInfo.split(":")(idPos).toLong
    }
    catch {
      case e: NumberFormatException =>
    }
    res
  })

  def cutArray(number: Int): UserDefinedFunction = udf((eventInfo: scala.collection.mutable.WrappedArray[String]) => {
    val len = if (eventInfo.length > number) eventInfo.length - number else 0
    eventInfo.dropRight(len)
  })

  def cutArray: UserDefinedFunction = udf((eventInfo: scala.collection.mutable.WrappedArray[String]) => {
    val len = if (eventInfo.length > 20) eventInfo.length - 20 else 0
    eventInfo.dropRight(len)

  })

  def cutArrayAndToString(prefix: String): UserDefinedFunction = udf((eventInfo: scala.collection.mutable.WrappedArray[String]) => {
    val len = if (eventInfo.length > 20) eventInfo.length - 20 else 0
    prefix.concat(eventInfo.dropRight(len).mkString(","))

  })


  def toString1: UserDefinedFunction = udf((value: Long) => {
    String.valueOf(value)
  })

  def toLong: UserDefinedFunction = udf((value: String) => {
    value.toLong
  })

  def splitVideosLength: UserDefinedFunction = udf((events: String) => {
    Array(events.split(",").length)
  })


}
