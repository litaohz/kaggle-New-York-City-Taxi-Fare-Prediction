package com.netease.music.algo.event.compute

import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.functions.explode

import com.netease.music.algo.event.path.OutputPath._
import com.netease.music.algo.event.AffinityScore._
import com.netease.music.algo.event.schema.BaseSchema._
import com.netease.music.algo.event.udf.ComputeScore.{computeFinalScore1, computeTscore}
import com.netease.music.algo.event.udf.RankAgg
import org.apache.spark.sql.{DataFrame, SaveMode}
import com.netease.music.algo.event.udf.ComputeScore._

object SnsBase {
  var cal1: Calendar = Calendar.getInstance()
  cal1.add(Calendar.DATE, -60)
  val timeThreshold1 = cal1.getTimeInMillis / 1000L

  /*

  res5: Long = 355396294->272293208
   */
  def baseInfo(args: Array[String]): org.apache.spark.sql.DataFrame = {
    import spark.implicits._
    val follow = spark.read.option("sep", "\t").schema(followSchema).csv("/db_dump/music_ndir/Music_Follow")
    val artist = spark.read.option("sep", "\t").schema(artistSchema).csv("/user/ndir/db_dump_music/Musician_Account")
    val phoneFriend = spark.read.option("sep", "\001").schema(schemaPhoneFriend).
      csv("hive_db/ndir_201712_unicom_tmp.db/dump_join_external_friends/ds=".concat(args(0)))
    val star = spark.read.option("sep", "\001").schema(schemaStar).
      csv("hive_db/ndir_201712_unicom_tmp.db/dump_evt_user_star1/ds=".concat(args(0)))

    follow.where("friendid !=1 and friendid != 9003 and friendid != 48353").createOrReplaceTempView("follow0")
    phoneFriend.where("get_json_object(json, '$.type') = 1").createOrReplaceTempView("phone1")
    star.createOrReplaceTempView("star")
    hc.cacheTable("star")
    artist.createOrReplaceTempView("artist")
    hc.cacheTable("artist")
    val v1 = spark.sql("select a.userid,a.friendid,min(a.createtime) as createtime from " +
      "(select userid,friendid,createtime from follow0)a" +
      " group by  a.userid,a.friendid").withColumn("time_dist", timeDistance($"createtime"))
    v1.createOrReplaceTempView("follow1")
    v1
  }

  /*
  +---------+-----------+
  ","creatorid","active_time","
  +---------+-----------+
  ","105759563"," 1518629428","
  ","360746944"," 1519467794","
  ","103558426"," 1519199586","
  "," 83989691"," 1519270248","

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
    spark.sql("select a.userid, a.friendid,a.time_dist from(select userid, friendid,time_dist from follow1)a " +
      "join (select  userid from evt where event_count >0 and status=0)b on a.friendid = b.userid").createOrReplaceTempView("follow2")
  }

  def fansInfo(): Unit = {

    spark.sql("select friendid,count(1) as cnt from follow1 group by friendid").createOrReplaceTempView("fansInfo")
  }

  /*
  todo:去重
   */
  def getSearchInfo(ds: String): Unit = {
    spark.read.parquet(searchInfoRead).where("datediff(ds,'".concat(ds).concat("')>-7")).
      createOrReplaceTempView("searchInfo")

  }

  /*
  71618951
   */
  def songInfo(ds: String, tableName: String): Unit = {
    import spark.implicits._
    songInfo0(ds).
      createOrReplaceTempView(tableName)

  }

  def songInfo0(ds: String): DataFrame = {
    import spark.implicits._
    //split
    spark.read.option("sep", "\t").schema(commonSchema5).csv("music_recommend/event/user_play_song_artistId/".concat(getNdaysShift(ds, -1))).union(
      spark.read.option("sep", "\t").schema(commonSchema5).csv("music_recommend/event/user_play_song_artistId/".concat(getNdaysShift(ds, -3)))).union(
      spark.read.option("sep", "\t").schema(commonSchema5).csv("music_recommend/event/user_play_song_artistId/".concat(getNdaysShift(ds, -5)))).union(
      spark.read.option("sep", "\t").schema(commonSchema5).csv("music_recommend/event/user_play_song_artistId/".concat(getNdaysShift(ds, -7))))

  }

  /*
  45226230
   */
  def weiboInfo(): Unit = {
    import spark.implicits._
    spark.read.option("sep", "\t").schema(commonSchema6).csv("music_recommend/event/music_friend_statistic/").
      createOrReplaceTempView("weiboInfo")

  }


  def getNdaysShift(ds0: String, shift: Int): String = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance
    calendar.setTime(dateFormat.parse(ds0))
    calendar.add(Calendar.DAY_OF_MONTH, shift)
    dateFormat.format(calendar.getTime)
  }

  /*
  最后还需要distinct
   */

  def getTotalInfo(): DataFrame = {

    val inSide = ((friendid: String, friendids: String) => {
      var res = false
      if (friendids != null) {
        res = friendids.split(",").contains(friendid)

      }
      res
    })
    spark.udf.register("inside", inSide)

    spark.sql("select a.userid, a.friendid, a.time_dist, " +
      "case when (b.friendid is not null and b.userid is not null) then 1 else 0 end as from_phone, " +
      "case when c.friendid is not null then 1 else 0 end as from_artist, " +
      "case when d.friendid is not null then 1 else 0 end as from_star, " +
      "case when (e.friendid is not null and e.userid is not null) then 1 else 0 end as from_search, " +
      "case when (f.userid is not null and inside(a.friendid, f.friendids)) then 1 else 0 end as from_song, " +
      "case when (g.userid is not null and inside(a.friendid, g.friendids)) then 1 else 0 end as from_weibo " +
      "  from " +
      "  (select userid,friendid,time_dist from follow2) a left outer join  " +
      "  (select userid,friendid, json  from phone1) b on (a.userid = b.userid and a.friendid = b.friendid) left outer join " +
      "  (select userid as friendid  from artist) c on a.friendid = c.friendid left outer join " +
      "  (select userid as friendid  from star) d on a.friendid = d.friendid left outer join " +
      "  (select userid , friendid  from searchInfo) e on (a.userid = e.userid and a.friendid = e.friendid) left outer join " +
      "  (select userid , friendids  from songInfo) f on a.userid = f.userid left outer join " +
      "  (select userid , friendids  from weiboInfo) g on a.userid = g.userid "
    ).dropDuplicates(Seq("userid", "friendid", "time_dist", "from_phone", "from_artist", "from_star", "from_search", "from_song", "from_weibo"))

  }

  def main(args: Array[String]): Unit = {
    println(getNdaysShift("2018-05-22",1))

  }
}
