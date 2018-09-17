package com.netease.music.algo.event.top.features

import com.netease.music.algo.event.analyse.AddFollow.spark
import com.netease.music.algo.event.compute.SnsBase.getNdaysShift
import com.netease.music.algo.event.path.DeletePath.delete
import com.netease.music.algo.event.path.OutputPath._
import com.netease.music.algo.event.schema.BaseSchema._
import com.netease.music.algo.event.udf.ComputeCscoreAgg
import com.netease.music.algo.event.udf.ComputeScore._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.netease.music.algo.event.udf.Udf1._
import com.netease.music.algo.event.v1.Path.impressAddFollow
/*
 */
object Interaction {

  val conf = new SparkConf()
  val spark = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    delete(spark, interactionPath.concat(getNdaysShift(args(0),-10)))

    val evt = spark.read.option("sep", "\t").schema(clickLogSchema).
      csv("/user/ndir/music_recommend/event/mainpage_log/".concat(args(0)).concat("/event/")).filter("length(cretorid) < 20")
    evt.createOrReplaceTempView("evt")

    val result = spark.sql("select cast(user_id as long),  cast(cretorid as long), count(1) as cnt  from evt where action = 'eventclick' group by  user_id,  cretorid ")
    result.write.mode(SaveMode.Overwrite).parquet(interactionPath.concat(args(0)))
  }
}
