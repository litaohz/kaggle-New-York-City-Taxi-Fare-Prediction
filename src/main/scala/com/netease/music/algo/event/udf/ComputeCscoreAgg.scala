package com.netease.music.algo.event.udf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types._
import com.netease.music.algo.event.udf.Utils._


class ComputeCscoreAgg extends UserDefinedAggregateFunction {
  def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("input_score", StringType) :: Nil)

  def bufferSchema: StructType = StructType(
    StructField("cnt", StringType) :: Nil)


  def dataType: DataType = StringType

  def deterministic: Boolean = true

  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = ""
  }

  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val map = new scala.collection.mutable.HashMap[String, Int]

    val str = buffer.getAs[String](0)
    str.split(",").map(getX12).foreach(x => updateMap1(x._1, x._2, map))
    val str1 = input.getAs[String](0)
    str1.split(",").map(getX12).foreach(x => updateMap1(x._1, x._2, map))
    buffer(0) = map.mkString(",").replace("->", ":").replace(" ", "")
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val map = new scala.collection.mutable.HashMap[String, Int]

    val str = buffer1.getAs[String](0)
    str.split(",").map(getX12).foreach(x => updateMap1(x._1, x._2, map))
    val str1 = buffer2.getAs[String](0)
    str1.split(",").map(getX12).foreach(x => updateMap1(x._1, x._2, map))
    buffer1(0) = map.filter(x => x._1 != "-1").mkString(",").replace("->", ":").replace(" ", "")

  }

  /*
      f"$t%1.8f".toDouble
  定义用户间互动参数C
  统计对用户在7天内产生的互动次数（赞，评论，转发，按次数计算）x
  C = log(x+1) + 1 若当天刚关注，则取C = 1
  需要配置参数： C01 = 10 （对数底数，决定半衰期）
   */
  def evaluate(buffer: Row): Any = {
    val str = buffer.getString(0)
    val map = new scala.collection.mutable.HashMap[String, Int]
    str.split(",").map(getX12).foreach(x => updateMap1(x._1, x._2, map))
    map.map(x => (x._1, Math.log10(x._2 + 1) + 1)).map(x => (x._1, f"${x._2}%1.8f".toDouble)
    ).mkString(",").replace("->", ":").replace(" ", "")
  }
}
