package com.netease.music.algo.event

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.netease.music.algo.event.udf.Udf1._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.netease.music.algo.event.path.OutputPath._
object VideoPrefRetriver {


  def main(args: Array[String]): Unit = {
    println("hello-".concat(args(0)))
    val videoPath = "music_recommend/feed_video/getVideoPool_output/parquet"
    val eventPath = "music_recommend/event/getEventResourceInfoForEventPool_eventInfo/parquet"
    val outputPath = "music_recommend/event/video_pref_transfer/"
    val constrain = args(1)

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

    val result0 = videoPref.
      withColumn("videoInfo", explode(splitVideosWIthConstrain(Integer.parseInt(constrain))($"videos"))).drop($"videos").
      withColumn("videoId", extractVideoId(0)($"videoInfo")).filter("videoId > 0").select("userId", "videoId").
      join(videoDim.filter($"eventId" > 0).drop("creatorId").join(eventDim.filter($"eventId" > 0), "eventId").
        select("eventId", "videoId", "artistId", "topicId","creatorId"), "videoId")
    val result = result0.
      select($"userId", concat_ws(":", $"eventId", $"videoId", $"artistId", $"topicId",$"creatorId").alias("eventInfo")).
      groupBy("userId").agg(collect_list($"eventInfo").alias("eventInfo")).
      select($"userId", concat_ws(",", $"eventInfo").alias("eventInfo")).
      select(concat_ws("\t", $"userId", $"eventInfo"))
    result0.write.mode(SaveMode.Overwrite).parquet(videoPrefPath)
    result.repartition(50).write.option("compression","gzip").mode(SaveMode.Overwrite).text(outputPath.concat(args(0)))
  }

}
