package com.netease.music.algo.event.v1

import com.netease.music.algo.event.v1.Functions._
import com.netease.music.algo.event.v1.Path._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/*
evt: 2852776
uid: 922756
有1249个人，在1个月内，能够发布100个以上的动态,36603个人，10个动态以上，，81859是5个以上，924612是0个以上,
其中，大于10个动态认证用户，有685个,认证用户，本身是有63217个

 */
object AuthenticatedUser {

  val conf = new SparkConf()
  val spark = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    import spark.implicits._
    val limit = getNdaysShift(args(0), -30)
    val authenticated = spark.read.parquet("/user/da_music/hive/warehouse/music_db_front.db/music_authenticateduser")

    spark.read.parquet("/user/da_music/hive/warehouse/music_dimension.db/event_meta_info_history/dt=".concat(args(0))).
      filter($"create_time" > limit).
      createOrReplaceTempView("event")
    /*
  WHEN '31' THEN 'share_comment' -- 分享评论 160540
    WHEN '38' THEN 'share_concert' -- 演出 1007
    WHEN '41' THEN 'share_video' -- 分享视频 1202013
    WHEN '22' THEN 'forward' -- 转发 814724
    WHEN '19' THEN 'share_album' -- 分享专辑 46101
    WHEN '21' THEN 'share_mv' -- 分享MV 138897
    WHEN '17' THEN 'share_djprogram' -- 分享电台节目 183889
    WHEN '24' THEN 'share_column' -- 分享专栏 11671
    WHEN '28' THEN 'share_djradio' -- 分享电台 13792
    WHEN '13' THEN 'share_playlist' -- 分享歌单  248230
    WHEN '36' THEN 'share_artist' -- 分享艺人 7546
    WHEN '39' THEN 'pub_video' -- 发布视频 187851
     */
    spark.sql("select *," +
      " case when event_type='share_comment'     then   31 " +
      " when event_type='share_concert'     then   38 " +
      " when event_type='share_video'       then   41 " +
      " when event_type='forward'           then   22 " +
      " when event_type='share_album'       then   19 " +
      " when event_type='share_mv'          then   21 " +
      " when event_type='share_djprogram'   then   17 " +
      " when event_type='share_column'      then   24 " +
      " when event_type='share_djradio'     then   28 " +
      " when event_type='share_playlist'    then   13 " +
      " when event_type='share_artist'      then   36 " +
      " when event_type='pub_video'         then   39 end as type from event").where(
      "type in (38,41,22,19,21,17,24,28,13,36,39)").createOrReplaceTempView("event1")

    spark.sql("select a.user_id, a.cnt from (select user_id,count(1) as cnt from event1 group by user_id)a where a.cnt < 5000 and a.cnt > 10").
      write.mode(SaveMode.Overwrite).parquet(puidCandidates.concat(args(0)))
  }


}
