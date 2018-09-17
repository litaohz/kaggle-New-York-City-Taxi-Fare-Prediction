package com.netease.music.algo.event.analyse

import com.netease.music.algo.event.path.OutputPath._
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{udf, _}
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.netease.music.algo.event.analyse.AnalyseOutput._
/*
 spark.sql("select *,cast (props['time'] as bigint) as duration from view\n" +
      "where props['type']='end'\n" +
      "and props['time'] between 0 and 18000 \n" +
      "and props['id'] in ('NMFriendsViewController','tracktimeline')")
 */
object FEAnalyse3 {
  def main(args: Array[String]): Unit = {
    import spark.implicits._
    val imp = spark.read.parquet("/user/da_music/hive/warehouse/music_dw.db/user_action/dt=".concat(args(0)).concat("/action=impress"))

    imp.createOrReplaceTempView("imp")
    val impCount = spark.sql("select userid,props['resourceid'] as eventid from imp \n" +
      "where props['target']='event'\n" +
      "and props['scene'] ='event' \n" +
      "and props['page'] ='recommendpersonal' and userid=105666254").
      select(concat_ws("\t", $"userid", $"eventid")).repartition(1). write.mode(SaveMode.Overwrite).text("data/debug")

    val clk = spark.read.parquet("/user/da_music/hive/warehouse/music_dw.db/user_action/dt=".concat(args(0)).concat("/action=follow")).
      where("props['scene'] ='event' and props['page'] ='recommendpersonal' ")
    val clkCount = clk.where("props['target'] != 'dislike'").select("userid").distinct().count()
    clk.createOrReplaceTempView("clk")
    spark.sql("select " + impCount +" as imp_cnt, " + clkCount +" as clk_cnt, " +
      "props['target'] as type, count(distinct userid) as cnt  from clk \n" +
      "group by  props['target']").
      write.mode(SaveMode.Overwrite).parquet(feAnalyseOutput.concat(args(0)))
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
