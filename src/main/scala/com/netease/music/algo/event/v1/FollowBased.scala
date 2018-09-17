package com.netease.music.algo.event.v1

import com.netease.music.algo.event.schema.BaseSchema.followSchema
import com.netease.music.algo.event.v1.Path._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}


object FollowBased {


/*
9003
1
48353
1305093793
 */

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    import spark.implicits._
    spark.read.option("sep", "\t").schema(followSchema).csv("/db_dump/music_ndir/Music_Follow").
      filter("friendid not in(1,9003,48353,1305093793)").createOrReplaceTempView("follow")
    spark.read.parquet("/user/da_music/hive/warehouse/music_db_front.db/music_authenticateduser").createOrReplaceTempView("auth")
    spark.read.parquet("/user/da_music/hive/warehouse/music_db_front.db/music_authenticateduser").
      where("userid not in(1,9003,48353,1305093793)").select("userid").repartition(1).
      write.mode(SaveMode.Overwrite).text(authDim)
    val pre = spark.sql("select a.userid, a. friendid, 1 as score from (select * from follow )a " +
      " join (select userid from auth)b on a.friendid = b.userid")

    //for cf
      pre.select(concat_ws("\t", $"userid", $"friendid", $"score")).
      write.mode(SaveMode.Overwrite).text(followCfPre)

    //for embedding
    pre.groupBy("userid").agg(collect_list($"friendid").alias("friendids")).
      select($"userid", concat_ws("\t", $"friendids").alias("friendids")).
      select("friendids").
      write.mode(SaveMode.Overwrite).text(followCfPre1)
    /*
    /user/ndir/music_recommend/itembased/filter/ItemSimilarityJob/artist_netease_withMusician
     */

  }
}
