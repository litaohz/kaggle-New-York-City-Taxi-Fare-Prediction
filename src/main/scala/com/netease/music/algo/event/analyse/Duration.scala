package com.netease.music.algo.event.analyse

import com.netease.music.algo.event.path.OutputPath._
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.netease.music.algo.event.udf.UserActionUdf._
import org.apache.spark.sql.functions._

/*
 spark.sql("select *,cast (props['time'] as bigint) as duration from view\n" +
      "where props['type']='end'\n" +
      "and props['time'] between 0 and 18000 \n" +
      "and props['id'] in ('NMFriendsViewController','tracktimeline')")
 */
object Duration {
  def main(args: Array[String]): Unit = {
    import spark.implicits._
    val view = spark.read.parquet("/user/da_music/hive/warehouse/music_dw.db/user_action_fast/dt=".concat(args(0)).concat("/action=view"))
      view.createOrReplaceTempView("view")
    spark.sql("select userid, cast (props['time'] as bigint) as durations  from view\n" +
      "where props['type']='end'\n" +
      "and props['time'] between 0 and 18000 \n" +
      "and props['id'] in ('NMFriendsViewController','tracktimeline')").
      groupBy("pos").agg(sum($"label").as("label")).
      write.mode(SaveMode.Overwrite).parquet(durationPath.concat(args(0)))

  }

  val conf = new SparkConf()
  val spark = SparkSession
    .builder()
    .config(conf).enableHiveSupport()
    .getOrCreate()

  def getTarget: UserDefinedFunction = udf((map: scala.collection.immutable.Map[String, String]) => {
    map.get("target")

  })
}
