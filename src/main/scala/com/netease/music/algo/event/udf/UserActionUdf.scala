package com.netease.music.algo.event.udf

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object UserActionUdf {

  def getProp(prop: String): UserDefinedFunction = udf((map: scala.collection.immutable.Map[String, String]) => {
    map.get(prop)

  })


  def getProp1(prop: String): UserDefinedFunction = udf((map: scala.collection.immutable.Map[String, String]) => {
    map.get(prop).asInstanceOf[Long]

  })

  def main(args: Array[String]): Unit = {
    println(1+Math.log10(1+21))
  }
}
