package com.netease.music.algo.event.top.features

import com.netease.music.algo.event.path.OutputPath._
import com.netease.music.algo.event.schema.BaseSchema._
import com.netease.music.algo.event.top.features.Udf._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
/*
zhao
 */
object Search {
  def main(args: Array[String]): Unit = {
    import spark.implicits._
    //好多无效数据
    val song2artist = spark.read.option("sep", "\t").schema(commonSchema3).csv("/db_dump/music_ndir/Music_Song_artists").
      where("artists_info is not null").
      withColumn("artistid",getArtist($"artists_info")).where("artistid > 0")
    val artist = spark.read.option("sep", "\t").schema(artistSchema).csv("/user/ndir/db_dump_music/Musician_Account").
      withColumnRenamed("userid","friendid")


    val searchedSong = spark.read.option("sep", "#").schema(commonSchema4).
      csv("/user/ndir/music_search/song/user_action/hour/".concat(args(0)).concat("-*")).
      where("userid > 0").withColumn("songid",getSongId($"songinfo")).where("songid is not null and length(songid) > 1")
    searchedSong.join(song2artist,"songid").join(artist,"artistid").select("userid","friendid","artistid")
    .write.mode(SaveMode.Overwrite).parquet(searchInfo.concat(args(0)))
  }

  val conf = new SparkConf()
  val spark = SparkSession
    .builder()
    .config(conf).enableHiveSupport()
    .getOrCreate()

}
