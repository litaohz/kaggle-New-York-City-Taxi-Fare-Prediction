package com.netease.music.algo.event.landingpage.candidates

import java.util.{Arrays, Collections}

import com.netease.music.algo.event.Rank.FriendIds
import com.netease.music.algo.event.compute.SnsBase.getNdaysShift
import com.netease.music.algo.event.landingpage.candidates.Artist.appendStr
import com.netease.music.algo.event.path.OutputPath._
import com.netease.music.algo.event.schema.BaseSchema.{commonSchema5, followSchema}
import com.netease.music.algo.event.udf.ComputeScore.getToday
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.netease.music.algo.event.v1.Path._
/*
todo: 处理下c_score为null的情况
 */
object Normal1 {

  val conf = new SparkConf()
  val spark = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()

  val hc = new org.apache.spark.sql.hive.HiveContext(spark.sparkContext)


  def main(args: Array[String]): Unit = {
    import spark.implicits._

    val reason = "2"
    val alg = "v2"
    val str = "-".concat(reason).concat(":").concat(alg)
    val timestampStr = "=".concat(getToday)
    spark.read.option("sep", "\t").schema(commonSchema5).csv("music_recommend/event/user_play_song_artistId_long/".concat(args(0))).
      withColumn("friendid", explode(splitFriends($"friendids"))).drop("friendids").createOrReplaceTempView("rec")
    spark.read.parquet(impressAddFollow1.concat(args(0))).
      union(spark.read.parquet(impressAddFollow1.concat(getNdaysShift(args(0), -1)))).
      union(spark.read.parquet(impressAddFollow1.concat(getNdaysShift(args(0), -2)))).
      createOrReplaceTempView("impress")
    spark.read.option("sep", "\t").schema(followSchema).csv("/db_dump/music_ndir/Music_Follow").createOrReplaceTempView("follow")

    val recommend = spark.sql("select a.userid, a.friendid from " +
      "(select userid, friendid from rec)a left outer join (select userid, friendid from impress)b " +
      "on a.userid=b.userid and a.friendid = b.friendid" +
      " left outer join (select userid, friendid from follow)c " +
      "on a.userid=c.userid and a.friendid = c.friendid " +
      "where b.friendid is null and c.friendid is null")
    recommend.withColumn("friendid", appendStr(str)($"friendid")).
      groupBy("userid").agg(collect_list($"friendid").alias("friendids")).
      select($"userId", concat_ws(",", $"friendids").alias("friendids")).withColumn("friendids", appendStr(timestampStr)($"friendids")).

      select(concat_ws("\t", $"userId", $"friendids")).
      write.mode(SaveMode.Overwrite).option("compression", "gzip").text(landingPageOutputNormal.concat(args(0)))

  }

  def splitFriends: UserDefinedFunction = udf((friendids: String) => {
    friendids.split(",")
  })


  val insertSth = ((res0: String, friendId: String) => {
    var result = ""
    val index = res0.indexOf(",")
    if (index > 0 && index < res0.length - 1) {
      result = res0.substring(0, index + 1).concat(friendId).concat(",").concat(res0.substring(index + 1, res0.length))

    }
    else
      result = res0
  })


}
