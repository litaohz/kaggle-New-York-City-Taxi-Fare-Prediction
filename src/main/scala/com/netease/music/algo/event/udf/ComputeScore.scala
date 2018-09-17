package com.netease.music.algo.event.udf

import java.util
import java.util.Calendar

import com.netease.music.algo.event.udf.Utils._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import java.util.Collections
import java.util.Arrays

import org.apache.spark.ml

import scala.collection.JavaConverters._


object ComputeScore {
  var cal: Calendar = Calendar.getInstance()
  cal.add(Calendar.DATE, -14)
  val timeThreshold = cal.getTimeInMillis / 1000L

  def computeFinalScore: UserDefinedFunction = udf((a: Double, t: Double, c: Double, u: Double) => {

    val score = a * t * c * u

    f"${score}%1.8f".toDouble
  })


  def reOrder: UserDefinedFunction = udf((result: String, friendids: Seq[String]) => {
    var res = result
    val index1 = result.indexOf(",")
    val index2 = result.indexOf(",", index1 + 1)
    if (friendids != null && index1 != -1 && index2 != -1) {
      res = result.drop(index2 + 1)
      var leftStr = ""
      var rightStr = ""
      var friendid = result.substring(0, index1)
      if (!friendids.contains(friendid)) {
        leftStr = leftStr.concat(friendid).concat(",")
      }
      else {
        rightStr = rightStr.concat(",").concat(friendid)
      }
      friendid = result.substring(index1 + 1, index2)
      if (!friendids.contains(friendid)) {
        leftStr = leftStr.concat(friendid).concat(",")
      }
      else {
        rightStr = rightStr.concat(",").concat(friendid)
      }
      res = leftStr.concat(res).concat(rightStr)
    }
    res
  })


  def computeFinalScore1: UserDefinedFunction = udf((a: Double, t: Double) => {

    val score = a * t

    f"${score}%1.8f".toDouble
  })

  def normalizeScore(mean: Double, stddev: Double): UserDefinedFunction = udf((score: Double) => {
    val score1 = (score - mean) / stddev
    f"${score1}%1.8f"
  })

  def persistScore: UserDefinedFunction = udf((a: Double, t: Double, c: Double, u: Double) => {
    val arr = Array(a, t, c, u)
    arr.mkString(",")
  })

  def check: UserDefinedFunction = udf((a: Any) => {
    a != null
  })

  /*
   */
  def computeTscore(logBase: Int): UserDefinedFunction = udf((createTime: String) => {
    val v0 = createTime.toLong
    val now = System.currentTimeMillis()
    val v1 = (now - v0) / 86400 / 1000 + 1
    val score = 1.toDouble / (1 + Math.log10(v1))
    f"${score}%1.8f".toDouble

  })

  def timeDistance: UserDefinedFunction = udf((createTime: String) => {
    (System.currentTimeMillis() - createTime.toLong) / 86400 / 1000
  })



  def toLong: UserDefinedFunction = udf((str: String) => {
    var res = 0L
    try {
      res = str.toLong
    }
    catch {
      case e: Exception =>
    }
    res
  })


  def timeDistance1: UserDefinedFunction = udf((createTime: String) => {
    (System.currentTimeMillis()/1000 - createTime.toLong) / 86400
  })

  def getId(splitter: String): UserDefinedFunction = udf((ids: String) => {
    ids.split(splitter)
  })


  def computeUscore(logBase: Int): UserDefinedFunction = udf((createTime: Long) => {
    var score = 1.0
    val v0 = createTime
    val now = System.currentTimeMillis() / 1000L
    val v1 = (now - v0) / 86400
    if (v1 > 4) {
      score = 1.toDouble / (1 + Math.log(v1) / Math.log(logBase))
    }
    f"${score}%1.8f".toDouble
  })


  def computeSize: UserDefinedFunction = udf(f = (map: scala.collection.immutable.Map[String, Int]) => {
    map.size
  })

  def transMapScore: UserDefinedFunction = udf((mScore: scala.collection.immutable.Map[String, Int]) => {
    mScore.mkString(",").replace("->", ":").replace(" ", "")
  })

  def computeCscore: UserDefinedFunction = udf((userId: String, friendid: String, creatorid: Long, mScoreStr: String) => {
    var mScore = 1.toDouble
    if (friendid.toLong == creatorid) {
      val map1 = new scala.collection.mutable.HashMap[String, Double]
      mScoreStr.split(",").map(getX12).foreach(x => updateMap2(x._1, x._2, map1))
      if (map1.contains(userId)) {
        mScore = map1(userId)
      }
    }
    mScore
  })


  def computeIscore: UserDefinedFunction = udf((cnt: Long, cnt4: Long, cnt7: Long, cnt15: Long, cnt31: Long) => {
    100 * cnt + cnt4 * 10 + cnt7 * 5 + cnt15 * 2 + cnt31
  })

  def vectorHead = udf((x: ml.linalg.DenseVector) => x(1))

  /*
   */
  def computeMscore: UserDefinedFunction = udf(f = (zanTimeStr: String, commentTimeStr: String, forwardTimeStr: String) => {
    val map = new scala.collection.mutable.HashMap[String, Int]
    if (zanTimeStr != null && !zanTimeStr.isEmpty) {
      zanTimeStr.split(",").map(getX12).filter(_._1 != "-1").filter(_._2.toLong > timeThreshold).map(_._1).foreach(x => updateMap(x, map))
    }
    if (commentTimeStr != null && !commentTimeStr.isEmpty) {
      commentTimeStr.split(",").map(getX12).filter(_._2.toLong > timeThreshold).map(_._1).foreach(x => updateMap(x, map))
    }
    if (forwardTimeStr != null && !forwardTimeStr.isEmpty) {
      forwardTimeStr.split(",").map(getX12).filter(_._2.toLong > timeThreshold).map(_._1).foreach(x => updateMap(x, map))
    }
    map
  })


  def cutAndAppend = udf((result: String) => {
    val resArray = result.split(",")
    val res = if (resArray.length - 2000 > 0)  resArray.dropRight(resArray.length - 2000) else resArray

    res.map(x=>{
      x.concat(":o")
    }).mkString(",").concat("-").concat(getToday).concat("-").concat(getToday)
  })

  def doAppendAlg(patchStr: String, ds: String, alg1: String, alg2: String) = udf((result: String, friendids: String) => {
    val needNum = 8
    val resArray = result.split(",").map(x => {
      x.concat(":").concat(alg1)
    })
    var res = resArray.mkString(",")

    val patchNum = needNum - resArray.length
    if (patchNum > 0) {

      val patchList = Arrays.asList(friendids.split(","): _*)
      Collections.shuffle(patchList)
      res = res.concat(",").concat(patchList.asScala.dropRight(patchList.size() - patchNum).map(x => {
        String.valueOf(x).concat(":").concat(alg2)
      }).mkString(","))
    }
    res
  })


  def mergeRes = udf((res1: String, res2: String) => {
    res1.concat(",").concat(res2).concat("-").concat(getToday).concat("-").concat(getToday)
  })


  def getRecArtist = udf((artistStrWithScore: String) => {
    artistStrWithScore.split(",").map(x => {
      x.split(":")(0).concat(":").concat("cf")
    }).mkString(",")
  })


  def doCut(patchStr: String, ds: String, alg1: String, alg2: String) = udf((result: String) => {
    val needNum = 8
    val resArray = result.split(",").map(x => {
      x.concat(":").concat(alg1)
    })
    var res = resArray.mkString(",")
    if (resArray.length > 2000) {

      res = resArray.dropRight(resArray.length - 2000).mkString(",")
    }

    res
  })

  def getToday: String = {
    val cal = Calendar.getInstance
    cal.set(Calendar.HOUR_OF_DAY, 0)
    cal.set(Calendar.SECOND, 0)
    cal.set(Calendar.MINUTE, 0)
    cal.set(Calendar.MILLISECOND, 0)
    String.valueOf(cal.getTimeInMillis)
  }


  def main(args: Array[String]): Unit = {
    val patchStr = "29001,124182034,59086,33033"
    val result = "59086:o,58371:o,29001:o"
    var res = result
    val needNum = 5
    val resArray = result.split(",")
    var patchNum = needNum - resArray.length
    val patchList = Arrays.asList(patchStr.split(","): _*)
    Collections.shuffle(patchList)
    var i = 0

    while (patchNum > 0 && i < patchList.size()) {
      val patch = patchList.get(i)

      if (!result.contains(patch)) {

        res = res.concat(",").concat(patch).concat(":").concat("p")
        patchNum = patchNum - 1
      }

      i = i + 1
    }
    println(res)
  }
}
