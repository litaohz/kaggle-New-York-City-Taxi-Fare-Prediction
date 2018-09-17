package com.netease.music.algo.event.FE51

import com.netease.music.algo.event.path.InputPath.sptEvent
import com.netease.music.algo.event.path.OutputPath._
import com.netease.music.algo.event.schema.DumpSchema.sptEvtSchema
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{collect_list, concat_ws, explode}
import com.netease.music.algo.event.schema.BaseSchema._
import com.netease.music.algo.event.udf.Udf1.{extractVideoId, splitVideos}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import Udf._
object RecOperatorPool {

  def main(args: Array[String]): Unit = {
    import spark.implicits._
    spark.read.json("/db_dump/music_ndir/Music_RcmdFeedBlock").where("blockType=8").
      withColumn("resourceId", getResIdFromContent($"content")).
      withColumn("resourceType", getResTypeFromContent($"content")).where("resourceType=24").
      withColumn("songId", explode(getSongId($"songIds"))).
      withColumn("artistId", explode(getSongId($"artistIds"))).select("resourceId","songId","id","artistId","blockType").
      createOrReplaceTempView("bizpool")
    val event = spark.read.option("sep", "\t").schema(commonSchema).csv("/user/ndir/music_recommend/event/re_sort_output_new").
      where("result is not null")
    /*
    应该用creatorArtistId？
     */
    spark.read.parquet("music_recommend/event/getEventResourceInfoForEventPool_eventInfo/parquet").
      where("isMusicEvent = 1 and artistId >0 and isControversialFigure=0").
      select("eventId", "artistId","creatorid","resourceId","songId").
      createOrReplaceTempView("algopool")
    val pool = spark.sql("select a.eventid,b.blockType,b.id from (select eventId,artistId,creatorid,resourceId,songId from algopool)a " +
      "join " +
      "(select artistId,resourceId,songId,id,blockType from bizpool)b" +
      " on (a.resourceId=b.resourceId or a.artistId=b.artistId or a.resourceId = b.resourceId)")
    event.withColumn("eventInfo", explode(splitVideos($"result"))).drop($"result").
      withColumn("eventId", extractVideoId(0)($"eventInfo")).drop($"eventInfo").
      filter("eventId > 0").join(pool, "eventId").
      select($"userId", concat_ws1( $"id",$"blockType").alias("eventInfo")).
      groupBy("userId").agg(collect_list($"eventInfo").alias("eventInfo")).
      select($"userId", concat_ws(",", $"eventInfo").alias("eventInfo")).
      select(concat_ws("\t", $"userId", $"eventInfo")).
      write.option("compression", "gzip").mode(SaveMode.Overwrite).text(evtFEOutputBiz.concat("ds=")concat(args(0)))
  }

  val conf = new SparkConf()
  val spark = SparkSession
    .builder()
    .config(conf).enableHiveSupport()
    .getOrCreate()

}
