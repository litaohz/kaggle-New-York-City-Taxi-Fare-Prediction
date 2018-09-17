package com.netease.music.algo.event.analyse

import com.netease.music.algo.event.schema.BaseSchema
import com.netease.music.algo.event.udf.Udf1._
import com.netease.music.algo.event.schema.BaseSchema._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import com.netease.music.algo.event.analyse.UdfBI._
object BasicAnalyse {
  val outputPath = "music_recommend/moments/sns/landingpage/normal//2018-05-12"
  def main(args: Array[String]): Unit = {
    println("hello")
    val conf = new SparkConf()
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    import spark.implicits._
    val outputPath = ("music_recommend/moments/sns/landingpage/cf/2018-06-03")
    val outputSchema = StructType(Array(StructField("userId", StringType, nullable = false), StructField("videos", StringType, nullable = false)))
    val videoPref = spark.read.option("sep", "\t").schema(outputSchema).csv(outputPath)
    val videoPrefTab = videoPref.
      withColumn("videoInfo", explode(splitVideos($"videos"))).drop($"videos").
      withColumn("eventId", extractVideoId(0)($"videoInfo")).
      withColumn("videoId", extractVideoId(1)($"videoInfo")).
      withColumn("artistId", extractVideoId(2)($"videoInfo")).
      withColumn("topicId", extractVideoId(3)($"videoInfo")).
      select($"eventId",$"videoId", $"artistId", $"topicId")
    videoPrefTab.createOrReplaceTempView("videoPref")
    spark.sql("select count(distinct eventId), count(distinct videoId),count(distinct artistId),count(distinct topicId) from videoPref").show(10)

    spark.read.option("sep", "\t").schema(commonSchema).csv(outputPath).withColumn("ok",check($"result")).where("ok = true").show
  }

}
