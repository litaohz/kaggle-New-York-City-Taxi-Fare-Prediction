package com.netease.music.algo.event

import com.netease.music.algo.event.path.OutputPath._
import com.netease.music.algo.event.compute.Sns1._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
/*
被关注着黑名单
9003
1
48353
follow.where("friendid !=1 and friendid != 9003 and friendid != 48353").count--326671911

关注者黑名单：
282220145

 res0.where("c_score is not null").count
res1: Long = 121071001

todo: 处理下c_score为null的情况
 */
object AffinityScore1 {

  val conf = new SparkConf()
  val spark = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()

  val hc = new org.apache.spark.sql.hive.HiveContext(spark.sparkContext)

  def main(args: Array[String]): Unit = {

    computeUscore(args, spark.read.parquet(outputPathAT)).
      write.mode(SaveMode.Overwrite).parquet(outputPathATU)

  }
}
