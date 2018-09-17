package com.netease.music.algo.event.support

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.netease.music.algo.event.schema.DumpSchema._
import com.netease.music.algo.event.path.InputPath._
import com.netease.music.algo.event.path.OutputPath._
import org.apache.spark.sql.functions.{collect_list, concat_ws}
object Spt1 {

  def main(args: Array[String]): Unit = {
    import spark.implicits._

    val event = spark.read.option("sep", "\001").schema(sptEvtSchema).csv(sptEvent).filter("creatorid is not null")
    val video = spark.read.parquet(videoPrefPath)
    val result = video.join(event, Array("creatorid","eventid")).
      select($"userId", concat_ws(":", $"eventId", $"videoId", $"artistId", $"topicId",$"creatorid").alias("eventInfo")).
      groupBy("userId").agg(collect_list($"eventInfo").alias("eventInfo")).
      select($"userId", concat_ws(",", $"eventInfo").alias("eventInfo")).
      select(concat_ws("\t", $"userId", $"eventInfo"))
    result.write.option("compression","gzip").mode(SaveMode.Overwrite).text(evtSnsOutput.concat(args(0)))

  }
  val conf = new SparkConf()
  val spark = SparkSession
    .builder()
    .config(conf).enableHiveSupport()
    .getOrCreate()

}
