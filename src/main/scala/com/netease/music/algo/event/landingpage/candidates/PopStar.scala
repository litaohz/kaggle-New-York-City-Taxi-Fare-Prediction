package com.netease.music.algo.event.landingpage.candidates

import com.netease.music.algo.event.compute.SnsBase._
import com.netease.music.algo.event.landingpage.candidates.Normal1.spark
import com.netease.music.algo.event.path.OutputPath._
import com.netease.music.algo.event.udf.ComputeScore.getToday
import com.netease.music.algo.event.udf.Udf1._
import com.netease.music.algo.event.schema.BaseSchema.commonSchema1
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{collect_set, concat_ws, explode, udf}
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.netease.music.algo.event.v1.Path._

/*
40932973
 */
object PopStar {

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
    val videoPath = "music_recommend/feed_video/getVideoPool_output/parquet"
    /*
    530353
     */
    val videoDim = spark.read.parquet(videoPath)
    import org.apache.spark.sql.types._

    val videoSchema = StructType(Array(StructField("userId", StringType, nullable = false), StructField("videos", StringType, nullable = false)))
    val videoPref = spark.read.option("sep", "\t").schema(videoSchema).csv("/user/ndir/music_recommend/feed_video/re_sort_output_active")
    spark.read.parquet(impressAddFollow1.concat(args(0))).
      union(spark.read.parquet(impressAddFollow1.concat(getNdaysShift(args(0), -1)))).
      union(spark.read.parquet(impressAddFollow1.concat(getNdaysShift(args(0), -2)))).
      createOrReplaceTempView("impress")
    /*
    61941
     */
    val popStar = spark.read.option("sep", "\001").
      schema(commonSchema1).csv("/user/ndir/hive_db/ndir_201712_unicom_tmp.db/pop_star/ds=".concat(args(0))).withColumnRenamed("userId", "creatorId")
    /*
    pair:
     */
   videoPref.
      withColumn("videoInfo", explode(splitVideos(($"videos")))).drop($"videos").
      withColumn("videoId", extractVideoId(0)($"videoInfo")).filter("videoId > 0").select("userId", "videoId").
      join(videoDim, "videoId").select("userId", "creatorId").join(popStar, "creatorid").
     withColumnRenamed("creatorid", "friendid").createOrReplaceTempView("rec")
    val result = spark.sql("select a.userid, a.friendid from " +
      "(select userid, friendid from rec)a left outer join (select userid, friendid from impress)b " +
      "on a.userid=b.userid and a.friendid = b.friendid where b.friendid is null")
    val reason = "1"
    val alg = "v1"
    val str = "-".concat(reason).concat(":").concat(alg)
    val timestampStr = "=".concat(getToday)
    result.withColumn("friendid", appendStr(str)($"friendid")).
      groupBy("userid").agg(collect_set($"friendid").alias("friendids")).
      select($"userId", concat_ws(",", $"friendids").alias("friendids")).withColumn("friendids", appendStr(timestampStr)($"friendids")).

      select(concat_ws("\t", $"userId", $"friendids")).
      write.mode(SaveMode.Overwrite).option("compression", "gzip").text(landingPageOutputPopStar.concat(args(0)))

    //7天内有发动态，看看人有多少

  }


  def appendStr(appendStr: String) = udf((result: String) => {
    result.concat(appendStr)
  })

  def splitFriends: UserDefinedFunction = udf((friendids: String) => {
    friendids.split(",")
  })
}
