package com.netease.music.algo.event.analyse

import com.netease.music.algo.event.AffinityScore.spark
import com.netease.music.algo.event.analyse.FEAnalyse3.spark
import com.netease.music.algo.event.schema.BaseSchema
import com.netease.music.algo.event.udf.Udf1._
import com.netease.music.algo.event.schema.BaseSchema._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.explode
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import com.netease.music.algo.event.analyse.UdfBI._
import com.netease.music.algo.event.backup.AffinityScoreBak.spark
import com.netease.music.algo.event.compute.SnsBase.getNdaysShift
import com.netease.music.algo.event.top.features.Interaction.spark
object VideoFollowRate {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    import spark.implicits._
    spark.read.parquet("/user/da_music/hive/warehouse/music_dw.db/user_action/dt=".concat("2018-05-17").concat("/action=follow")).
      createOrReplaceTempView("follow0")
    val follow = spark.sql("select userid, props['id'] as friendid from follow0")
    val outputPath = ("/user/ndir/music_recommend/feed_video/pre_log/2018-05-22/impress/")

    val videoInfo = spark.read.parquet("/user/ndir/music_recommend/feed_video/getVideoPool_output/parquet")
    spark.read.option("sep", "\t").schema(videoSchema).csv(outputPath).
      join(videoInfo,"videoId").
      select("userid","creatorid").withColumnRenamed("creatorid","friendid").join(follow, Seq("userid","friendid")).select("userid","friendid").
      distinct().count()
    spark.read.option("sep", "\t").schema(clickLogSchema).csv("/user/ndir/music_recommend/event/mainpage_log/2018-05-17/event/"). createOrReplaceTempView("evt")


    spark.sql("select user_id as userid,  cretorid as friendid from evt where action = 'eventimpress'").join(follow, Seq("userid","friendid")).select("userid","friendid").
      distinct().count()
    val result = spark.sql("select user_id,  cretorid as friendid from evt where action = 'eventclick' group by  user_id,  cretorid ")


    spark.read.option("sep", "\t").schema(clickLogSchema).csv("/user/ndir/music_recommend/event/mainpage_log/".concat(getNdaysShift(args(0), 1))).union(
      spark.read.option("sep", "\t").schema(clickLogSchema).csv("/user/ndir/music_recommend/event/mainpage_log/".concat(getNdaysShift(args(0), 2)))).union(
      spark.read.option("sep", "\t").schema(clickLogSchema).csv("/user/ndir/music_recommend/event/mainpage_log/".concat(getNdaysShift(args(0), 3)))).union(
      spark.read.option("sep", "\t").schema(clickLogSchema).csv("/user/ndir/music_recommend/event/mainpage_log/".concat(getNdaysShift(args(0), 4)))).union(
      spark.read.option("sep", "\t").schema(clickLogSchema).csv("/user/ndir/music_recommend/event/mainpage_log/".concat(getNdaysShift(args(0), 5))))


    val evt = spark.read.option("sep", "\t").schema(clickLogSchema).
      csv("/user/ndir/music_recommend/event/mainpage_log/".concat(args(0)).concat("/event/")).filter("length(cretorid) < 20")
    
  }


  

}
