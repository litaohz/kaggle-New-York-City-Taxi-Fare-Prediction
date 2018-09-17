package com.netease.music.algo.event.analyse

import com.netease.music.algo.event.analyse.UdfBI.check
import com.netease.music.algo.event.schema.BaseSchema.commonSchema
import com.netease.music.algo.event.udf.Udf1.{extractVideoId, splitVideos}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object TopFeatureAnalyse {
  def main(args: Array[String]): Unit = {
    println("hello")
    val conf = new SparkConf()
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    import spark.implicits._
    spark.read.parquet("music_recommend/event/sns/affinity/at/").createOrReplaceTempView("feature")
    spark.sql("select COUNT(CASE WHEN from_phone=1 THEN 1 ELSE NULL END) AS from_phone,  " +
      "COUNT(CASE WHEN from_artist=1 THEN 1 ELSE NULL END) AS from_artist,"  +
      "COUNT(CASE WHEN from_star=1 THEN 1 ELSE NULL END) AS from_star,"  +
      "COUNT(CASE WHEN from_artist=1 THEN 1 ELSE NULL END) AS from_artist," +
      "COUNT(CASE WHEN from_search=1 THEN 1 ELSE NULL END) AS from_search," +
      "COUNT(CASE WHEN from_weibo=1 THEN 1 ELSE NULL END) AS from_weibo," +
      "COUNT(CASE WHEN from_song=1 THEN 1 ELSE NULL END) AS from_song,count(1) as total from feature" ).show(10)
  }
}
