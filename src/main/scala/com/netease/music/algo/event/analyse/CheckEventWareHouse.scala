package com.netease.music.algo.event.analyse

import com.netease.music.algo.event.udf.Udf1.{extractVideoId, splitVideos}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.reflect.runtime.{universe => ru}

object CheckEventWareHouse {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    import spark.implicits._
    //    val outputPath = "music_recommend/event/merge_outputNew"
    val num = spark.read.parquet("music_recommend/event/event_feature_warehouse").
      where("refreshEventClickRate_2 is not null").count()
    if(num > 0){
      System.exit(0)
    }
    System.exit(1)
  }
}
