package com.netease.music.algo.event

import com.netease.music.algo.event.path.OutputPath._
import com.netease.music.algo.event.compute.SnsBase._
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

发不过动态的人数：14997934
 */
object AffinityScore {

  /*
  +----------+-----------+---------+-----------+-----------+----------+---------+---------+
|from_phone|from_artist|from_star|from_artist|from_search|from_weibo|from_song|    total|
+----------+-----------+---------+-----------+-----------+----------+---------+---------+
|  13386701|   99745857| 17697740|   99745857|     952930|   5923794| 23418834|314379550|
+----------+-----------+---------+-----------+-----------+----------+---------+---------+

   */
  def main(args: Array[String]): Unit = {

    baseInfo(args)
    filterInactive(args)
    getSearchInfo(args(0))
    songInfo(args(0),"songInfo")
    weiboInfo()
    getTotalInfo().write.mode(SaveMode.Overwrite).parquet(outputPathAT)
  }


  val conf = new SparkConf()
  val spark = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()
  val hc = new org.apache.spark.sql.hive.HiveContext(spark.sparkContext)


}
