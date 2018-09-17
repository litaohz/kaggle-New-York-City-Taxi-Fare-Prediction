package com.netease.music.algo.event.landingpage.candidates

import com.netease.music.algo.event.Rank.FriendIds
import com.netease.music.algo.event.landingpage.candidates.Artist.appendStr
import com.netease.music.algo.event.path.OutputPath._
import com.netease.music.algo.event.schema.BaseSchema._
import com.netease.music.algo.event.udf.ComputeScore.getToday
import com.netease.music.algo.event.udf.Udf1._
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.netease.music.algo.event.v1.Path._
/*
40932973
 */
object User2User {

  val conf = new SparkConf()
  val spark = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()

  /*
  先看看可能喜欢他们视频的人，动态，歌曲相关的东西，后面再看
  fid-reason:alg,....=timestamp
   */
  def main(args: Array[String]): Unit = {
    import spark.implicits._
    val path1 = "music_recommend/event/userbased/output/UserSimilarityJob/follow/"
    val path2 = "music_recommend/event/userbased/output/UserSimilarityJob/followcomment/"
    /*
    530353
     */
    val reason1 = "4"
    val alg = "v1"
    val str1 = "-".concat(reason1).concat(":").concat(alg)
    val timestampStr = "=".concat(getToday)

    val artist = spark.read.parquet("/user/da_music/hive/warehouse/music_db_front.db/music_registeredartist/")
    artist.select("userid","artistid"). select(concat_ws("\t", $"userid", $"artistid")).
      repartition(2).write.mode(SaveMode.Overwrite).text(u2aDim)
    artist.select("userid","artistid").
      join(spark.read.parquet("/user/da_music/hive/warehouse/music_dw.db/artist_meta_info_history/dt=".concat(args(1))).
        withColumnRenamed("id","artistid").select("artistid","name"),"artistid").
      select(concat_ws("\t", $"userid", $"name")).
      repartition(2).write.mode(SaveMode.Overwrite).text(u2NameDim)

    val artist2artist = spark.read.option("sep", "\t").schema(commonSchema8).
      csv("/user/ndir/music_recommend/itembased/filter/ItemSimilarityJob/artist_netease_withMusician").
      join(artist,"artistid").select("userid","result")

    artist2artist.withColumn("result", explode(splitVideos($"result"))).
      withColumn("artistid", extractVideoId(0)($"result")).join(artist.withColumnRenamed("userid","friendid"),"artistid").
      withColumn("friendid", appendStr(str1)($"friendid")).select("userid","friendid").
      groupBy("userId").agg(collect_list($"friendid").alias("friendid")).
      select($"userId", concat_ws(",", $"friendid").as("friendid")).withColumn("friendid", appendStr(timestampStr)($"friendid")).
      select(concat_ws("\t", $"userid", $"friendid")).
      write.mode(SaveMode.Overwrite).option("compression", "gzip").text(landingPageOutputCF.concat(args(0)))

    //7天内有发动态，看看人有多少

  }


  def appendStr(appendStr: String) = udf((result: String) => {
    result.concat(appendStr)
  })

  def splitFriends: UserDefinedFunction = udf((friendids: String) => {
    friendids.split(",")
  })
}
