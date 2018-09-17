package com.netease.music.algo.event.udf

import scala.collection.mutable

object Utils {
  def updateMap1(x: String, v: String, map: scala.collection.mutable.HashMap[String, Int]): Unit = {
    map.put(x, if (map.contains(x)) map(x) + v.toInt else v.toInt)
  }

  def updateMap2(x: String, v: String, map: scala.collection.mutable.HashMap[String, Double]): Unit = {
    map.put(x, v.toDouble)
  }


  def updateMap3(x: String, v: Double, map: scala.collection.mutable.HashMap[String, Double]): Unit = {
    map.put(x, v.toDouble)
  }
  def updateMap4(_1: String, _2: String, map: mutable.HashMap[String, (Int, Int)]):  Unit = {

  }


  def updateMap(x: String, map: scala.collection.mutable.HashMap[String, Int]): Unit = {
    map.put(x, if (map.contains(x)) map(x) + 1 else 1)
  }

  def mergeMap(map: scala.collection.mutable.HashMap[String, Int], map1: scala.collection.mutable.HashMap[String, Int]): Unit = {
    for (elem <- map) {
      if (map1.contains(elem._1)) {
        val count = elem._2 + map1(elem._1)
        map1.put(elem._1, count)
      }
      else {
        map1.put(elem._1, elem._2)
      }
    }
  }

  def getX12(x: String): (String, String) = {
    val xValue = x.split(":")
    if (xValue.length == 2)
      (xValue(0), xValue(1))
    else ("-1", "0")
  }

  def main(args: Array[String]): Unit = {
    val str = "2444409662:9:893259:251654915,3498389262:9:893259:1344105258,2433278479:9:893259:90562321,3465896382:9:6462:95161139,2440041301:9:5001:330434258,3502052326:9:12760978:304620685,2443818799:9:3685:127836080,3485229207:9:1137098:513920143,2399752066:9:12146142:320123275,3471921307:9:2159:318470247,3502293220:9:5774:539923903,3460794880:9:2159:492688094,2266337187:9:5001:95076,3449024752:9:6066:621063978,2428290925:9:784453:101262078,3480670113:9:6066:344996275,3495407248:9:13112043:535402244,2438224997:9:784453:250988491,3502757371:9:12932368:396277923,2439240231:9:12146142:40695951,2428550857:9:840134:250988491"
    println(str.split(",").length)
  }
}
