package com.netease.music.algo.event.udf

import com.netease.music.algo.event.udf.Utils._
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types._


class RankAgg extends UserDefinedAggregateFunction {
  def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("friendis", StringType) :: StructField("score", DoubleType) :: Nil)
  def bufferSchema: StructType = StructType(
    StructType(StructField("scores", StringType) :: Nil))
  def dataType: DataType = StringType
  def deterministic: Boolean = true
  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = ""
  }
  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val map = new scala.collection.mutable.HashMap[String, Double]
    val str = buffer.getAs[String](0)
    val friendid1 = input.getAs[String](0)
    val score1 = input.getAs[Double](1)
    map.put(friendid1, score1)
    str.split(",").map(getX12).foreach(x => updateMap3(x._1, x._2.toDouble, map))
    buffer(0) = map.filter(x => x._1 != "-1").mkString(",").replace("->", ":").replace(" ", "")
  }
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val map = new scala.collection.mutable.HashMap[String, Double]
    val str = buffer1.getAs[String](0)
    str.split(",").map(getX12).foreach(x => updateMap3(x._1, x._2.toDouble, map))
    val str1 = buffer2.getAs[String](0)
    str1.split(",").map(getX12).foreach(x => updateMap3(x._1, x._2.toDouble, map))
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
    val map = new scala.collection.mutable.HashMap[String, Double]
    str.split(",").map(getX12).filter(x => x._1 !="-1").foreach(x => updateMap3(x._1, x._2.toDouble, map))
    map.toSeq.sortWith(_._2 > _._2).map(_._1).mkString(",")
  }
}
