package com.netease.music.algo.event.online

import com.netease.music.algo.event.path.OutputPath._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object Clk {
  def main(args: Array[String]): Unit = {
    import spark.implicits._

    spark.read.parquet("/user/da_music/hive/warehouse/music_streaming.db/streaming_data/userActionLog_1525245000000").
    createOrReplaceTempView("data")
    val clk = spark.sql("select userid, props['id'] as friendid, " +
      "case when action='impress' then 1 else 0 end as impress, " +
      "case when action='click' then 1 else 0 end as click from data " +
      "where  " +
      " props['target']='userphoto' " +
      " and props['page']='eventpage' " +
      " and props['is_update'] is not null").
      groupBy("userid","friendid").agg(sum($"impress").as("impress"), sum($"click").as("click"))
    spark.sql("select case when cnt >0 then count(user_id) end as s1," +
      "case when cnt4 >0 then count(user_id) end  as s4," +
      "case when cnt7 >0 then count(user_id) end  as s7," +
      "case when cnt15 >0 then count(user_id) end  as s15," +
      "case when cnt31 >0 then count(user_id) end  as s31 from inter")
    clk.write.mode(SaveMode.Overwrite).parquet(batchClk)
  }

  val conf = new SparkConf()
  val spark = SparkSession
    .builder()
    .config(conf).enableHiveSupport()
    .getOrCreate()

}
