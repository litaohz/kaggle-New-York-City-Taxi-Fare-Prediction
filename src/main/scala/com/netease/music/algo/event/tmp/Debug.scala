package com.netease.music.algo.event.tmp

import com.netease.music.algo.event.path.OutputPath._
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object  Debug {
  def main(args: Array[String]): Unit = {
    import spark.implicits._

    val view = spark.read.parquet("/user/da_music/hive/warehouse/music_dw.db/user_action_fast/dt=2018-04-22").
      withColumn("props", map2str($"props"))
    view.createOrReplaceTempView("view")
    val clk = spark.sql("select userid,props from view " +
      "where  userid= 377167036 and action='impress' and " +
      "props['page']='eventpage'").take(100)
    spark.read.parquet("/user/da_music/hive/warehouse/music_streaming.db/streaming_data/userActionLog_1525245300000").
      where("userid=27005 and props['targetid']=3527363077").
    withColumn("props", map2str($"props")).select(concat_ws("\t", $"logtime", $"props")).repartition(1).write.mode(SaveMode.Overwrite).text("litao03/debug")

  }

  val conf = new SparkConf()
  val spark = SparkSession
    .builder()
    .config(conf).enableHiveSupport()
    .getOrCreate()


  def map2str: UserDefinedFunction = udf((info : Map[String, String]) => {
    info.mkString(",")
  })


}
