package com.netease.music.algo.event.landingpage.candidates

import com.netease.music.algo.event.landingpage.candidates.PopStar.{appendStr, spark}
import com.netease.music.algo.event.path.OutputPath._
import com.netease.music.algo.event.schema.BaseSchema.{commonSchema1, followSchema}
import com.netease.music.algo.event.udf.ComputeScore.getToday
import com.netease.music.algo.event.udf.Udf1.{extractVideoId, splitVideos}
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

/*
todo: 处理下c_score为null的情况
 */
object Normal {

  val conf = new SparkConf()
  val spark = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()

  val hc = new org.apache.spark.sql.hive.HiveContext(spark.sparkContext)


  def main(args: Array[String]): Unit = {
    import spark.implicits._
    val videoPath = "music_recommend/feed_video/getVideoPool_output/parquet"

    val videoDim = spark.read.parquet(videoPath)
    import org.apache.spark.sql.types._

    val videoSchema = StructType(Array(StructField("userId", StringType, nullable = false), StructField("videos", StringType, nullable = false)))
    val videoPref = spark.read.option("sep", "\t").schema(videoSchema).csv("/user/ndir/music_recommend/feed_video/re_sort_output_active")

    /*
    pair:
     */
    videoPref.
      withColumn("videoInfo", explode(splitVideos(($"videos")))).drop($"videos").
      withColumn("videoId", extractVideoId(0)($"videoInfo")).filter("videoId > 0").select("userId", "videoId").
      join(videoDim, "videoId").select("userId", "creatorId").createOrReplaceTempView("recommender")
    spark.read.option("sep", "\t").schema(followSchema).csv("/db_dump/music_ndir/Music_Follow").createOrReplaceTempView("follow")

    spark.sql("select a.userid, a.creatorid as friendid  from(select userid, creatorid from recommender)a " +
      " left outer join  (select userid, friendid from follow)b on a.userid=b.userid and a.creatorid = b.friendid where b.userid is null").
      groupBy("userid").agg(collect_list($"friendid").alias("friendids")).
      withColumn("friendid", dropFriends($"friendids")).
      write.mode(SaveMode.Overwrite).parquet(landingPageOutputNormalPre)

  }

  def dropFriends: UserDefinedFunction = udf((friendids: List[Long]) => {
    val num2Drop =  if (friendids.length > 1) friendids.length - 1 else 0
    friendids.dropRight(num2Drop).mkString(",")
  })

}
