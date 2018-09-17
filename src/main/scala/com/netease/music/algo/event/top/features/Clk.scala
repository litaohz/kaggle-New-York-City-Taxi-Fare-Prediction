package com.netease.music.algo.event.top.features

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import com.netease.music.algo.event.path.OutputPath._
import com.netease.music.algo.event.udf.UserActionUdf._
import org.apache.spark.sql.functions._

object Clk {
  def main(args: Array[String]): Unit = {
    import spark.implicits._

    val view = spark.read.parquet("/user/da_music/hive/warehouse/music_dw.db/user_action_fast/dt=".concat(args(0)))
    view.createOrReplaceTempView("view")
    val clk = spark.sql("select userid, props['id'] as friendid, " +
      "case when action='impress' then 1 else 0 end as impress, " +
      "case when action='click' then 1 else 0 end as click," +
      " props['alg'] as alg  from view " +
      "where  " +
      " props['target']='userphoto' " +
      " and props['page']='eventpage' " +
      " and props['is_update'] is not null" +
      " and props['alg'] is not null").
      groupBy("userid","friendid","alg").agg(sum($"impress").as("impress"), sum($"click").as("click"))

    clk.write.mode(SaveMode.Overwrite).parquet(clkRatePath.concat(args(0)))
  }

  val conf = new SparkConf()
  val spark = SparkSession
    .builder()
    .config(conf).enableHiveSupport()
    .getOrCreate()

}
