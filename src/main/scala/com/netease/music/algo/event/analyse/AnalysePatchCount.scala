package com.netease.music.algo.event.analyse

import com.netease.music.algo.event.analyse.UdfBI._
import com.netease.music.algo.event.compute.SnsPatch.getPatchData
import com.netease.music.algo.event.path.OutputPath.affinityRank1
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object AnalysePatchCount {
  val conf = new SparkConf()
  val spark = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()
  import spark.implicits._

  val output = "music_recommend/event/sns/output/"

  def main(args: Array[String]): Unit = {
    val patchStr = getPatchData(args)
    val result = spark.read.parquet(affinityRank1)
    result.filter(countArray($"result"))
  }

}
