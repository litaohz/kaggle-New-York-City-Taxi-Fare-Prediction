package com.netease.music.algo.event.analyse

import com.netease.music.algo.event.analyse.AnalyseOutput._
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{SaveMode, SparkSession}

/*
 spark.sql("select *,cast (props['time'] as bigint) as duration from view\n" +
      "where props['type']='end'\n" +
      "and props['time'] between 0 and 18000 \n" +
      "and props['id'] in ('NMFriendsViewController','tracktimeline')")
 */
object FriendPageAnalyse {
  def main(args: Array[String]): Unit = {
    val imp = spark.read.parquet("/user/da_music/hive/warehouse/music_dw.db/user_action_fast/dt=".concat("2018-05-23").concat("/action=eventimpress"))

    imp.createOrReplaceTempView("imp")
    spark.sql("select userid,props['id'] as eventid,props['sourceid'] as creatorid,props['alg'] as alg  from imp")
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
