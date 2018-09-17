package com.netease.music.algo.event.analyse

import com.netease.music.algo.event.compute.SnsBase.getNdaysShift
import com.netease.music.algo.event.path.DeletePath._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.netease.music.algo.event.v1.Path._

object AddFollow {
  val conf = new SparkConf()
  val spark = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    delete(spark, impressAddFollow.concat(getNdaysShift(args(0),-7)))

    val imp = spark.read.parquet("/user/da_music/hive/warehouse/music_dw.db/user_action_fast/dt=".concat(args(0)).concat("/action=impress"))
    imp.createOrReplaceTempView("imp")
    spark.sql("select userid, props['resourceid'] as friendid, props['alg'] as alg, " +
      "props['type'] as type from imp where props['page']='addfollow'").
      write.mode(SaveMode.Overwrite).parquet(impressAddFollow.concat(args(0)))



  }

}
