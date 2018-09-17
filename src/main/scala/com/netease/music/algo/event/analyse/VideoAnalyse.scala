package com.netease.music.algo.event.analyse

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import com.netease.music.algo.event.udf.Udf1._
import org.apache.spark.sql.functions._

object VideoAnalyse {
  def main(args: Array[String]): Unit = {
    val videoPath = "music_recommend/feed_video/getVideoPool_output/parquet"
    val eventPath = "music_recommend/event/getEventResourceInfoForEventPool_eventInfo/parquet"
    val outputPath = "music_recommend/event/video_pref_transfer/"


    val conf = new SparkConf()
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    import spark.implicits._

    val videoDim = spark.read.parquet(videoPath)
    val eventDim = spark.read.parquet(eventPath)
    import org.apache.spark.sql.types._

    val videoSchema = StructType(Array(StructField("userId", StringType, nullable = false), StructField("videos", StringType, nullable = false)))

    val videoPref = spark.read.option("sep", "\t").schema(videoSchema).csv("/user/ndir/music_recommend/feed_video/re_sort_output_active")

    val videoPrefTab = videoPref.withColumn("videoInfo", explode(splitVideosLength($"videos"))).drop($"videos")
    videoPrefTab.createOrReplaceTempView("videoPref")
    spark.sql("select count(case when videoInfo <10 then 1 end), " +
      " count(case when videoInfo >= 10 and videoInfo <20 then 1 end)," +
      "count(case when videoInfo >= 20 and videoInfo <30 then 1 end)," +
      "count(case when videoInfo >= 30 and videoInfo <40 then 1 end)," +
      "count(case when videoInfo >= 40 and videoInfo <50 then 1 end)," +
      "count(case when videoInfo >= 50 and videoInfo <60 then 1 end)," +
      "count(case when videoInfo >= 60 and videoInfo <70 then 1 end), " +
      "count(case when videoInfo >= 70 and videoInfo <80 then 1 end), " +
      "count(case when videoInfo >= 80 and videoInfo <90 then 1 end)," +
      "count(case when videoInfo >= 90 and videoInfo <100 then 1 end)," +
      "count(case when videoInfo >= 100 and videoInfo <150 then 1 end)," +
      "count(case when videoInfo >= 150 and videoInfo <200 then 1 end)," +
      "count(case when videoInfo >= 200 then 1 end) from videoPref").show(100)
  }
}
