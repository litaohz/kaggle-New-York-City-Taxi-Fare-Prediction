package com.netease.music.algo.event.compute

import com.netease.music.algo.event.AffinityScore.spark
import com.netease.music.algo.event.schema.BaseSchema._
import com.netease.music.algo.event.path.OutputPath._
import org.apache.spark.sql.functions.{collect_list, concat_ws}

object SnsPatch {

  def getPatchData(args: Array[String]): String = {
    spark.read.parquet(activePath).createOrReplaceTempView("active")
    import spark.implicits._

    spark.read.option("sep", "\001").schema(commonSchema1).csv(whiteListPath).where("userid is not null").
      createOrReplaceTempView("patch")
    new org.apache.spark.sql.hive.HiveContext(spark.sparkContext).cacheTable("patch")

    spark.sql("select a.creatorid from " +
      "(select creatorid from active)a  join" +
      "(select userid from patch)b on a.creatorid=b.userid ").
      agg(collect_list($"creatorId").alias("creatorId")).
      select(concat_ws(",", $"creatorId")).collect()(0).getString(0)
  }
}
