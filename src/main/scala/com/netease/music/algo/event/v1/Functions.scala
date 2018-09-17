package com.netease.music.algo.event.v1

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.collection
import scala.collection.mutable

object Functions {
  def getNdaysShift(ds0: String, shift: Int): Long = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance
    calendar.setTime(dateFormat.parse(ds0))
    calendar.add(Calendar.DAY_OF_MONTH, shift)
    calendar.getTimeInMillis / 1000L
  }

  def main(args: Array[String]): Unit = {
       println(  1112 /100)
  }


  def pickResult = udf((result: String) => {
    val res = result.split(",")
    val limit = 20
    res.dropRight(if (res.length - 1 < limit) 0 else (res.length - limit))
  })

  /*
  单条数据格式：userId	creatorId:reasonId1-reasonType1-alg1&reasonId2-reasonType2-alg2,creatorId:reasonId1-reasonType1-alg1
   */
  def matchString = udf((songid: String, creatorId: String) => {
    creatorId.concat(":").concat(songid).concat("-s").concat("-v1")
  })

  def test = udf((result: String) => {
    result.reverse
  })

  def trim = udf((result: String) => {
    result.trim
  })
  def toFilter = udf((eventid: String, filtered: String) => {
    if( filtered != null) filtered.split(",").contains(eventid) else true
  })

  /*
  2915298	104601777:108390-user-cf1,1390681736:108390-user-cf1&1493850928-user-cf1
   */
  def buildResult(official: mutable.Set[Long]): UserDefinedFunction = udf((result: String, friend: String, friendids: Seq[String],
                                                                           impressed: Seq[String]) => {
    val friendList =  if (friendids != null) friendids else Seq()
    val impressedList =  if (impressed != null) impressed else Seq()
    val tmp = result.split(";").map(x => {
      //      cf2:569617293,293811843,358213714,64647216,13959378,49201424,50178259,112024720,84147210,7455497, 1104594
      val alg = x.split(":")(0)
      val fids = x.split(":")(1)
      fids.split(",").filter(x => (!friendList.contains(x)) && (!official.contains(x.toLong)) && (!impressedList.contains(x))).map(y => {
        y.concat(":".concat(friend)).concat("-u-").concat(alg)
      }).mkString(",")
    })
    val res =  if (tmp.length < 200) tmp else tmp.dropRight(tmp.length - 200)
    res.mkString(",")
  })


  /*
  userId	creatorId:reasonId1-reasonType1-alg1&reasonId2-reasonType2-alg2,creatorId:reasonId-reasonType-alg
  */
  def buildFinalResult: UserDefinedFunction = udf((matches: Seq[String], friendids: Seq[String]) => {
    val friendList =  if (friendids != null) friendids else Seq()
    matches.flatMap(_.split(",")).filter(_.split(":").size == 2).map(x => {
      val res = x.split(":")
      (res(0), res(1))
    }).filter(x=>(!friendList.contains(x._1))).groupBy(_._1).map(x => {
      x._1.concat(":").concat(x._2.map(_._2).mkString("&"))
    }).mkString(",")
  })

  def testFinalResult: UserDefinedFunction = udf((matches: Seq[String]) => {
    matches.flatMap(_.split(",")).map(x => {
      x.split(":").size
    }).filter(_ < 2)
  })


  def test1: UserDefinedFunction = udf((result: String) => {
    result.split(",").filter(_.split(":").size != 2).mkString(",")
  })
}
