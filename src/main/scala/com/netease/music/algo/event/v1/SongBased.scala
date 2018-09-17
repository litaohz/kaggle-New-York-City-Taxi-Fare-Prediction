package com.netease.music.algo.event.v1

import com.netease.music.algo.event.compute.SnsBase.getNdaysShift
import com.netease.music.algo.event.schema.BaseSchema.commonSchema5
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SongBased {

  val conf = new SparkConf()
  val spark = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()
  def songInfo1(ds: String): DataFrame = {
    import spark.implicits._
    //split
    spark.read.option("sep", "\t").schema(commonSchema5).csv("music_recommend/event/user_play_song_artistId/".concat(getNdaysShift(ds, -1))).union(
      spark.read.option("sep", "\t").schema(commonSchema5).csv("music_recommend/event/user_play_song_artistId/".concat(getNdaysShift(ds, -4)))).union(
      spark.read.option("sep", "\t").schema(commonSchema5).csv("music_recommend/event/user_play_song_artistId/".concat(getNdaysShift(ds, -7)))).union(
      spark.read.option("sep", "\t").schema(commonSchema5).csv("music_recommend/event/user_play_song_artistId/".concat(getNdaysShift(ds, -10)))).union(
      spark.read.option("sep", "\t").schema(commonSchema5).csv("music_recommend/event/user_play_song_artistId/".concat(getNdaysShift(ds, -13)))).union(
      spark.read.option("sep", "\t").schema(commonSchema5).csv("music_recommend/event/user_play_song_artistId/".concat(getNdaysShift(ds, -16)))).union(
      spark.read.option("sep", "\t").schema(commonSchema5).csv("music_recommend/event/user_play_song_artistId/".concat(getNdaysShift(ds, -19)))).union(
      spark.read.option("sep", "\t").schema(commonSchema5).csv("music_recommend/event/user_play_song_artistId/".concat(getNdaysShift(ds, -22)))).union(
      spark.read.option("sep", "\t").schema(commonSchema5).csv("music_recommend/event/user_play_song_artistId/".concat(getNdaysShift(ds, -25)))).union(
      spark.read.option("sep", "\t").schema(commonSchema5).csv("music_recommend/event/user_play_song_artistId/".concat(getNdaysShift(ds, -28)))).union(
      spark.read.option("sep", "\t").schema(commonSchema5).csv("music_recommend/event/user_play_song_artistId/".concat(getNdaysShift(ds, -31))))

  }


  def main(args: Array[String]): Unit = {
    songInfo1(args(0)).createOrReplaceTempView("songInfo")
    /*
    /user/ndir/music_recommend/itembased/filter/ItemSimilarityJob/artist_netease_withMusician
     */

  }
}
