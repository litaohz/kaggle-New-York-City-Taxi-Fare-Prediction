package com.netease.music.algo.event.analyse

import com.netease.music.algo.event.udf.Udf1._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object BasicAnalyseAfterMerge {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    import spark.implicits._
        val outputPath = "music_recommend/event/merge_outputNew"
    val outputSchema = StructType(Array(StructField("userId", StringType, nullable = false), StructField("events", StringType, nullable = false)))
    val pref = spark.read.option("sep", "\t").schema(outputSchema).csv(outputPath)
    val prefTable = pref.
      withColumn("eventInfo", explode(splitResultAll($"events"))).drop($"events").
      withColumn("eventId", extractResult(0)($"eventInfo")).
      withColumn("type", extractResult(1)($"eventInfo")).
      select($"userId", $"eventId", $"type")
    prefTable.createOrReplaceTempView("pref")
    spark.sql("select count(distinct eventId),count(distinct userId), type from pref group by type").show(50)



  }
}
