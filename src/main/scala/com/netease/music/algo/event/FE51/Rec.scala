package com.netease.music.algo.event.FE51

import com.netease.music.algo.event.path.OutputPath._
import com.netease.music.algo.event.schema.BaseSchema._
import com.netease.music.algo.event.udf.Udf1._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{collect_list, concat_ws, explode}
import org.apache.spark.sql.{SaveMode, SparkSession}

object Rec {

  def main(args: Array[String]): Unit = {
    import spark.implicits._
    val artist = spark.read.option("sep", "\t").schema(artistSchema).csv("/user/ndir/db_dump_music/Musician_Account").
      select("userid","artistid").withColumnRenamed("userid","creatorid")

    val event = spark.read.option("sep", "\t").schema(commonSchema).csv("/user/ndir/music_recommend/event/re_sort_output_new").
      where("result is not null")
    val blacklist = "userid not in (105666254)"
    /*
    应该用creatorArtistId？
     */
    val pool = spark.read.parquet("music_recommend/event/getEventResourceInfoForEventPool_eventInfo/parquet").
      where("isMusicEvent = 1  and isControversialFigure=0").select("eventId","creatorid").
      join(artist,"creatorid")
    event.withColumn("eventInfo", explode(splitVideos($"result"))).drop($"result").
      withColumn("eventId", extractVideoId(0)($"eventInfo")).withColumn("fileType", extractVideoId(1)($"eventInfo")).drop($"eventInfo").
      filter("eventId > 0").where(blacklist).join(pool, "eventId").
      select($"userId", concat_ws(":", $"eventId", $"fileType", $"artistId",$"creatorid").alias("eventInfo")).
      groupBy("userId").agg(collect_list($"eventInfo").alias("eventInfo")).
      withColumn("eventInfo",cutArray($"eventInfo")).
      select($"userId", concat_ws(",", $"eventInfo").alias("eventInfo")).
      select(concat_ws("\t", $"userId", $"eventInfo")).
      write.option("compression", "gzip").mode(SaveMode.Overwrite).text(evtFEOutput.concat("ds=")concat(args(0)))

  }

  val conf = new SparkConf()
  val spark = SparkSession
    .builder()
    .config(conf).enableHiveSupport()
    .getOrCreate()

}
