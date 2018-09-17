package com.netease.music.algo.event.analyse

import com.netease.music.algo.event.udf.Udf1._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import com.netease.music.algo.event.schema.BaseSchema._

object FEAnalyse2 {
  def main(args: Array[String]): Unit = {
    println("hello")
    val conf = new SparkConf()
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    import spark.implicits._
    val output="music_recommend/event/getEventWithPics_fromEventMeta_output/eventWithPics"

    val res1 = spark.read.option("sep", "\t").schema(picSchema).
      csv(output)
    res1.where("pics_num=1").show(10)
    res1.where("pics_num=2").show(10)
    res1.where("pics_num=3").show(10)
    res1.where("pics_num=4").show(10)
    res1.where("pics_num=5").show(10)
    res1.where("pics_num=6").show(10)
    res1.where("pics_num=7").show(10)
    res1.where("pics_num=8").show(10)
    res1.where("pics_num=9").show(10)

  }
}
