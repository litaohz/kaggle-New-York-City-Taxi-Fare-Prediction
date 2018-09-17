package com.netease.music.algo.event.backup

import SnsBaseBak._
import com.netease.music.algo.event.backup.Sns1Bak.computeUscore
import com.netease.music.algo.event.path.OutputPath._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}


object AffinityScoreBak {

  val conf = new SparkConf()
  val spark = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()

  val outputPath = "music_recommend/event/sns/"
  val hc = new org.apache.spark.sql.hive.HiveContext(spark.sparkContext)

  def main(args: Array[String]): Unit = {

    filterCancelled(args)
    filterInactive(args)
    getTotalInfo()
    //    computeUscore(args, computeScore())
    computeUscore(args, computeScore()).write.mode(SaveMode.Overwrite).parquet(outputPathATUBak)
  }




}
