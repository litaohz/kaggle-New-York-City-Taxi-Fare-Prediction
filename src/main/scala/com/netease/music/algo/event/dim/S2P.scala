package com.netease.music.algo.event.dim

import com.netease.music.algo.event.v1.Path.followCfResLine
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{collect_list, concat_ws, explode, udf}
import Path._
object S2P {
  val conf = new SparkConf()
  val spark = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()
  case class S2Artist(songid: String, artists: Array[String])

  def main(args: Array[String]): Unit = {
  import  spark.implicits._
    val s2p = spark.sparkContext.textFile("/db_dump/music_ndir/Music_Song_artists/").map(line => {
        val info = line.split("\t")
        val songid = info(0)
        val artists =  info.drop(1).dropRight( if (info.length - 1 > 10) 0 else (info.length - 1 - 10))
        S2Artist(songid,artists)
      }).toDF().where("songid > 0 and size(artists) > 0").withColumn("artist", explode($"artists")).
      withColumn("artistid", getArtist($"artist")).
      join(spark.read.parquet("/user/da_music/hive/warehouse/music_db_front.db/music_authenticateduser").
        withColumnRenamed("registeredartistid","artistid"),"artistid").select("songid","userid")
    s2p. write.mode(SaveMode.Overwrite).parquet(S2PPathScatter)
      s2p.
      groupBy("songid").agg(collect_list($"userid").alias("userids")).
      select($"songid", concat_ws(",", $"userids").alias("userids")).
      select(concat_ws("\t", $"songid", $"userids")).repartition(50).
      write.mode(SaveMode.Overwrite).text(S2PPath)
  }

  def getArtist: UserDefinedFunction = udf((artist: String) => {
    artist.split(":")(0)
  })
}

