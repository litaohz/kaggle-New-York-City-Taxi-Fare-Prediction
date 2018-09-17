package com.netease.music.algo.event.backup

import java.util.Calendar

import com.netease.music.algo.event.backup.AffinityScoreBak._
import com.netease.music.algo.event.schema.BaseSchema._
import com.netease.music.algo.event.udf.ComputeScore.computeTscore


object SnsBaseBak {
  var cal1: Calendar = Calendar.getInstance()
  cal1.add(Calendar.DATE, -60)
  val timeThreshold1 = cal1.getTimeInMillis / 1000L
  /*

  res5: Long = 355396294->272293208
   */
  def filterCancelled(args: Array[String]): org.apache.spark.sql.DataFrame = {
    val follow = spark.read.option("sep", "\t").schema(followSchema).csv("/db_dump/music_ndir/Music_Follow")
    val artist = spark.read.option("sep", "\t").schema(artistSchema).csv("/user/ndir/db_dump_music/Musician_Account")
    val phoneFriend = spark.read.option("sep", "\001").schema(schemaPhoneFriend).
      csv("hive_db/ndir_201712_unicom_tmp.db/dump_join_external_friends/ds=".concat(args(0)))
    val star = spark.read.option("sep", "\001").schema(schemaStar).
      csv("hive_db/ndir_201712_unicom_tmp.db/dump_evt_user_star1/ds=".concat(args(0)))


    follow.where("friendid !=1 and friendid != 9003 and friendid != 48353").createOrReplaceTempView("follow1")
    phoneFriend.where("get_json_object(json, '$.type') = 1").createOrReplaceTempView("phone1")
    star.createOrReplaceTempView("star")
    hc.cacheTable("star")
    artist.createOrReplaceTempView("artist")
    hc.cacheTable("artist")
    follow
  }

  /*
  +---------+-----------+
  |creatorid|active_time|
  +---------+-----------+
  |105759563| 1518629428|
  |360746944| 1519467794|
  |103558426| 1519199586|
  | 83989691| 1519270248|

  res15: Long = 915158
  res18: Long = 1695907
  /*

res5: Long = 355396294->272293208,
spark.sql("select count(distinct friendid) as cnt from before")->38020484
spark.sql("select count(distinct friendid) as cnt from after")->9361342
 */
   */
  def filterInactive(args: Array[String]): Unit = {
    spark.read.parquet("/user/da_music/hive/warehouse/music_dw.db/user_meta_info_history/dt=".concat(args(1))).
      createOrReplaceTempView("evt")
    spark.sql("select a.userid, a.friendid,a.createtime from(select userid, friendid,createtime from follow1)a " +
      "join (select  userid from evt where event_count >0 and status=0)b on a.friendid = b.userid").createOrReplaceTempView("follow2")
  }

  def filterManyFans(musicFollow: org.apache.spark.sql.DataFrame): Unit = {

    // 计算用户粉丝数
    val musicFollowNum = musicFollow
      .groupBy("creatorId").count
      .select("creatorId", "count")
  }


  def getTotalInfo(): Unit = {
    val v0 = spark.sql("select a.userid, a.friendid, a.createtime, " +
      "case when a.friendid = b. friendid then 3 when a.friendid = b. friendid then 3 " +
      " when a.friendid = c. friendid then 3\nwhen a.friendid = d. friendid then 2" +
      " else 1 end as a_score  from " +
      "  (select userid,friendid,createtime from follow2) a left outer join  " +
      "  (select userid,friendid, json  from phone1) b on a.userid = b.userid left outer join " +
      "  (select userid as friendid  from artist) c on a.friendid = c.friendid left outer join " +
      "  (select userid as friendid  from star) d on a.friendid = d.friendid ")

    v0.createOrReplaceTempView("v0")
  }

  def computeScore(): org.apache.spark.sql.DataFrame = {
    import spark.implicits._
    val v1 = spark.sql("select userid, friendid, min(createtime) as createtime, max(a_score) as a_score from v0  group by userid, friendid")

    v1.withColumn("t_score", computeTscore(10)($"createtime")).drop($"createtime")

  }


}
