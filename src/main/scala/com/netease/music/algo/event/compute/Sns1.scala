package com.netease.music.algo.event.compute

import java.util.Calendar

import com.netease.music.algo.event.AffinityScore1._
import com.netease.music.algo.event.udf.ComputeScore._
import com.netease.music.algo.event.path.OutputPath._

object Sns1 {
  var cal1: Calendar = Calendar.getInstance()
  cal1.add(Calendar.DATE, -60)
  val timeThreshold1 = cal1.getTimeInMillis / 1000L
  import spark.implicits._



  def computeUscore(args: Array[String], v2: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame = {
    val mutual = spark.read.parquet(mutualPath)

    v2.createOrReplaceTempView("v2")
    mutual.createOrReplaceTempView("mutual")
    hc.cacheTable("mutual")

    /*
     */
    spark.sql("select a.* ," +
      "case when creatorid is not null then size else 0 end as evt_size , " +
      "case when creatorid is not null then evt_dist else 108 end as evt_dist from " +
      " (select * from v2)a " +
      "left outer join ( select * from mutual)b on a.friendid = b.creatorid ")

  }

}
