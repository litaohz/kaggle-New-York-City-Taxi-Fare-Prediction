package com.netease.music.algo.event.analyse

import com.netease.music.algo.event.analyse.UdfBI._
import com.netease.music.algo.event.schema.BaseSchema._
import com.netease.music.algo.event.udf.Udf1._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object SubButNotFollow {
  def main(args: Array[String]): Unit = {
    println("hello")
    val conf = new SparkConf()
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    import spark.implicits._
     spark.read.option("sep", "\t").schema(followSchema).csv("/db_dump/music_ndir/Music_Follow").createOrReplaceTempView("follow")
    val artist = spark.read.option("sep", "\t").schema(artistSchema).csv("/user/ndir/db_dump_music/Musician_Account")
    spark.read.json("/db_dump/music_ndir/Music_UserArtistSubscribe/2018-05-16").join(artist, "artistid").select("Userid","friendid").
      createOrReplaceTempView("sub")
    spark.sql("select a.* from (select * from follow)a left outer join(select * from sub)b on a.userid = b.userid and a.friendid = b.friendid where b.userid is null").show
  }

}
