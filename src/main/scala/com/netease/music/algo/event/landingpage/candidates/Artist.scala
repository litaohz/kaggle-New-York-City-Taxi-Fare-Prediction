package com.netease.music.algo.event.landingpage.candidates

import java.util.{Arrays, Collections}

import com.netease.music.algo.event.AffinityScore.spark
import com.netease.music.algo.event.AffinityScore1.spark
import com.netease.music.algo.event.compute.SnsBase._
import com.netease.music.algo.event.landingpage.candidates.Normal1.spark
import com.netease.music.algo.event.path.OutputPath._
import com.netease.music.algo.event.schema.BaseSchema.commonSchema5
import com.netease.music.algo.event.udf.ComputeScore.getToday
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{collect_set, concat_ws, explode, udf}
import com.netease.music.algo.event.v1.Path._

/*
40932973,用ferec还可以推荐她喜欢的动态
 */
object Artist {

  val conf = new SparkConf()
  val spark = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()

  /*
  fid-reason:alg,....=timestamp
   */
  def main(args: Array[String]): Unit = {
    import spark.implicits._
    spark.read.parquet(impressAddFollow1.concat(args(0))).
      union(spark.read.parquet(impressAddFollow1.concat(getNdaysShift(args(0), -1)))).
      union(spark.read.parquet(impressAddFollow1.concat(getNdaysShift(args(0), -2)))).
      createOrReplaceTempView("impress")
    val reason = "2"
    val alg = "v2"
    val str = "-".concat(reason).concat(":").concat(alg)
    val timestampStr = "=".concat(getToday)
    spark.read.option("sep", "\t").schema(commonSchema5).csv("music_recommend/event/user_play_song_artistId_long/".concat(args(0))).
      withColumn("friendid", explode(splitFriends($"friendids"))).drop("friendids").createOrReplaceTempView("rec")
    spark.read.parquet(impressAddFollow1.concat(args(0))).createOrReplaceTempView("impress")
    val recommend = spark.sql("select a.userid, a.friendid from " +
      "(select userid, friendid from rec)a left outer join (select userid, friendid from impress)b " +
      "on a.userid=b.userid and a.friendid = b.friendid where b.friendid is null")
    recommend.withColumn("friendid", appendStr(str)($"friendid")).
      groupBy("userid").agg(collect_set($"friendid").alias("friendids")).
      select($"userId", concat_ws(",", $"friendids").alias("friendids")).withColumn("friendids", appendStr(timestampStr)($"friendids")).

      select(concat_ws("\t", $"userId", $"friendids")).
      write.mode(SaveMode.Overwrite).option("compression", "gzip").text(landingPageOutputArtist.concat(args(0)))

    //7天内有发动态，看看人有多少

  }


  def appendStr(appendStr: String) = udf((result: String) => {
    result.concat(appendStr)
  })

  def splitFriends: UserDefinedFunction = udf((friendids: String) => {
    friendids.split(",").reverse.dropRight(3)
  })
}
