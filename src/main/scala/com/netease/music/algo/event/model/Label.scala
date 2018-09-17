package com.netease.music.algo.event.model

import com.netease.music.algo.event.path.OutputPath.{affinityRank, affinityRank1, affinityRankRollingUpdate, clkRatePath}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{collect_set, sum}
import com.netease.music.algo.event.path.OutputPath._
import Udf._
object Label {
  def main(args: Array[String]): Unit = {
    import spark.implicits._
    val clk = spark.read.parquet(clkRatePath.concat(args(0))).select("userid","friendid","click").groupBy("userid","friendid").
      agg(sum($"click").as("label")).withColumn("label",dealLabel($"label"))
    spark.read.parquet(outputPathATI.concat(args(1))).join(clk,Seq("userid","friendid")).
      write.mode(SaveMode.Overwrite).parquet(labelPath)
  }


  val conf = new SparkConf()
  val spark = SparkSession
    .builder()
    .config(conf).enableHiveSupport()
    .getOrCreate()


}
