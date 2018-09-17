package com.netease.music.algo.event.analyse

import com.netease.music.algo.event.schema.BaseSchema.commonSchema
import com.netease.music.algo.event.udf.Udf1.{extractVideoId, splitVideos}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import com.netease.music.algo.event.schema.DumpSchema._
import com.netease.music.algo.event.path.InputPath._
import org.apache.spark.sql.functions.desc

object CoverageAnalyse {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    import spark.implicits._
    //    val outputPath = "music_recommend/event/merge_outputNew"
    val outputPath1 = "music_recommend/event/merge_outputNew"
    val outputPath = ("music_recommend/event/category2levelRecall_output")

    val outputSchema = StructType(Array(StructField("userId", StringType, nullable = false), StructField("events", StringType, nullable = false)))

    val evtPref = spark.read.option("sep", "\t").schema(outputSchema).csv(outputPath)
    spark.read.option("sep", "\001").schema(sptEvtSchema).csv(sptEvent.concat(args(0))).
      createOrReplaceTempView("support")
    val evtPrefTab = evtPref.
      withColumn("eventInfo", explode(splitVideos($"events"))).drop($"events").
      withColumn("eventId", extractVideoId(0)($"eventInfo")).
      withColumn("type", extractVideoId(7)($"eventInfo")).
      select($"userId", $"eventId", $"type")
    evtPrefTab.createOrReplaceTempView("evtPref")
    spark.sql("SELECT count(distinct creatorid) as u,count(distinct eventid)as e FROM support").show(10)
    val missed = spark.sql("select a.creatorid, a.eventid, b.userid from (select creatorid,eventid from support)a left outer join" +
      " (select eventid,userid from evtPref)b on a.eventid = b.eventid where b.eventid is null")
    missed.createOrReplaceTempView("missed")
    spark.sql("select count(distinct eventid) as evt, count(distinct creatorid) as creator from missed ").show(10)
    val goaled = spark.sql("select a.creatorid, a.eventid, b.userid from (select creatorid,eventid from support)a left outer join" +
      " (select eventid,userid from evtPref)b on a.eventid = b.eventid where b.eventid is not null")
    missed.show(100)
    goaled.show(100)
    goaled.createOrReplaceTempView("goaled")
    spark.sql("select creatorid, eventid, count(1) as cnt from goaled group by creatorid, eventid").orderBy(desc("cnt")).show(100)
    spark.sql("select count(distinct userid) as u from goaled)").show(100)
  }

}
