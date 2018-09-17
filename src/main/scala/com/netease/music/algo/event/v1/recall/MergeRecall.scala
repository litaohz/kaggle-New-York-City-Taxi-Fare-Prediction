package com.netease.music.algo.event.v1.recall

import com.netease.music.algo.event.v1.Path._
import org.apache.spark.sql.functions.{collect_list, concat_ws}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import com.netease.music.algo.event.v1.Functions._
object MergeRecall {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate


    import spark.implicits._


    val followed = spark.read.parquet(tmpFollowAuth).groupBy("userid").agg(collect_list("friendid").as("friendids"))

    spark.read.parquet(matchResult).groupBy("userid").agg(collect_list("match").as("matches")).join(followed, Seq("userid"), "left_outer").
      withColumn("match",buildFinalResult($"matches",$"friendids")).select("userid","match").filter("match != ''").
      write.mode(SaveMode.Overwrite).parquet(matchResultAll)


  }





}
