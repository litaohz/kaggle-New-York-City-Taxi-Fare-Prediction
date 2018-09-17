package com.netease.music.algo.event.v1.recall

import java.io.InputStream

import com.netease.music.algo.event.compute.SnsBase.getNdaysShift
import com.netease.music.algo.event.path.DeletePath.delete
import com.netease.music.algo.event.schema.BaseSchema._
import com.netease.music.algo.event.udf.Udf1._
import com.netease.music.algo.event.v1.Path._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{collect_list, concat_ws, explode, udf}
import org.apache.spark.{SparkConf, SparkContext}
import com.netease.music.algo.event.v1.Functions.matchString
import com.netease.music.recommend.video.abtest.udfs.getABTestGroup

import scala.io.Source
object SongRecall {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate


    import spark.implicits._

    spark.read.parquet(songDim.concat(args(0))).where("artist_id > 0").withColumnRenamed("artist_id","registeredartistid").
      join(spark.read.parquet(authDimenson),"registeredartistid").
      select("id","userid").withColumnRenamed("id","songid").withColumnRenamed("userid","creatorid").
      write.mode(SaveMode.Overwrite).parquet(tmpS2u)

    spark.read.parquet("/user/da_music/hive/warehouse/music_dw.db/cloudmusic_official_account").
      createOrReplaceTempView("official")
  createImpressTable(spark, args)

    val songRecall =spark.read.option("sep", "\t").
      schema(commonSchema).csv(organicRecall).
      select("userid", "result").withColumn("result", explode(splitResultWithNumber(200)(($"result")))).
      withColumn("songid", extractResult(0)($"result")).drop("result")
    songRecall.join(spark.read.parquet(tmpS2u), "songid").select("userid","songid","creatorid").createOrReplaceTempView("song_recall")

    val songResult = spark.sql("select a.userid,a.songid, a.creatorid from (select userid,songid,creatorid from song_recall)a " +
      " left outer join(select userid, puid as creatorid from impressed) b on a.userid = b.userid  and a.creatorid = b.creatorid " +
      "left outer join(select userid as creatorid from official) c on a.creatorid = c.creatorid  " +
      " where b.creatorid is null and c.creatorid is null" +
      " and a.creatorid !=1452176465  ")
    songResult.withColumn("match",matchString($"songid", $"creatorid")).select("userid","match").
      write.mode(SaveMode.Overwrite).parquet(matchResult.concat("/type=song"))

  }

def createImpressTable(spark : SparkSession, args: Array[String]): Unit ={
  import spark.implicits._

  val stream1: InputStream = this.getClass.getResourceAsStream("/abtest_config")

  val configText = Source.fromInputStream(stream1).getLines().toArray
  val impressedTableC = spark.read.parquet(labelPre).
    filter(getABTestGroup(configText, "evt-alg-fatigue")($"userId") === "c").where("datediff(ds,'".concat(args(0)).concat("') > -7")).
    select("userid", "puid")

  val impressedTableT = spark.read.parquet(labelPre).
    filter(getABTestGroup(configText, "evt-alg-fatigue")($"userId") === "t").where("datediff(ds,'".concat(args(0)).concat("') > -30")).
    select("userid", "puid")

  val impressedTable = impressedTableC.union(impressedTableT)
  impressedTable.createOrReplaceTempView("impressed")
}



}
