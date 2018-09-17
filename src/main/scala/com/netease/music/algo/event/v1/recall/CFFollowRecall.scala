package com.netease.music.algo.event.v1.recall

import java.io.InputStream

import com.netease.music.algo.event.compute.SnsBase.getNdaysShift
import com.netease.music.algo.event.path.DeletePath.delete
import com.netease.music.algo.event.schema.BaseSchema._
import com.netease.music.algo.event.udf.Udf1.{extractResult, splitResultWithNumber}
import com.netease.music.algo.event.v1.Functions.matchString
import com.netease.music.algo.event.v1.Path._
import com.netease.music.algo.event.v1.link.predict.NodeBased1.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{collect_list, concat_ws, explode}
import com.netease.music.algo.event.v1.Functions.buildResult
import com.netease.music.recommend.video.abtest.udfs.getABTestGroup

import scala.collection.mutable
import scala.io.Source

object CFFollowRecall {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    /*
单行格式
userId	creatorId:reasonId1-reasonType1-alg1&reasonId2-reasonType2-alg2,creatorId:reasonId-reasonType-alg     */
    import spark.implicits._
    val officialSet = mutable.Set[Long]()

    val official = spark.read.parquet("/user/da_music/hive/warehouse/music_dw.db/cloudmusic_official_account").select("userid").collect().
      foreach(x => {
        officialSet += x.getLong(0)
      })

    officialSet += 1452176465
    val stream1: InputStream = this.getClass.getResourceAsStream("/abtest_config")
    val configText = Source.fromInputStream(stream1).getLines().toArray
    val impressedTableC = spark.read.parquet(labelPre).
      filter(getABTestGroup(configText, "evt-alg-fatigue")($"userId") === "c").where("datediff(ds,'".concat(args(0)).concat("') > -7")).
      select("userid", "puid")

    val impressedTableT = spark.read.parquet(labelPre).
      filter(getABTestGroup(configText, "evt-alg-fatigue")($"userId") === "t").where("datediff(ds,'".concat(args(0)).concat("') > -30")).
      select("userid", "puid")

    val impressedTable = impressedTableC.union(impressedTableT).
      groupBy("userid").agg(collect_list("puid").as("impressed"))
    val followed = spark.read.parquet(tmpFollowAuth).groupBy("userid").agg(collect_list("friendid").as("friendids"))
    val cf = spark.read.option("sep", "\t").schema(commonSchema).
      csv(followMergeResLine).withColumnRenamed("userid", "friendid")
    spark.read.parquet(tmpFollowAuth).
      join(cf, "friendid").select("userid", "result", "friendid").
      join(followed, Seq("userid"), "left_outer").
      join(impressedTable, Seq("userid"), "left_outer").
      withColumn("result", buildResult(officialSet)($"result", $"friendid", $"friendids", $"impressed")).
      select("userid", "result").withColumnRenamed("result", "match").
      write.mode(SaveMode.Overwrite).parquet(matchResult.concat("/type=follow"))
  }

}
