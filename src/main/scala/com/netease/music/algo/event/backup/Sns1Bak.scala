package com.netease.music.algo.event.backup

import java.util.Calendar

import com.netease.music.algo.event.AffinityScore1._
import com.netease.music.algo.event.path.OutputPath._
import com.netease.music.algo.event.udf.ComputeScore._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object Sns1Bak {
  var cal1: Calendar = Calendar.getInstance()
  cal1.add(Calendar.DATE, -60)
  val timeThreshold1 = cal1.getTimeInMillis / 1000L
  import spark.implicits._



  def computeUscore(args: Array[String], v2: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame = {
    val mutual = spark.read.parquet(mutualPathBak)

    v2.createOrReplaceTempView("v2")
    mutual.createOrReplaceTempView("mutual")
    hc.cacheTable("mutual")

    /*
     */
    spark.sql("select userid,friendid,a_score,t_score ," +
      "case when creatorid is not null then u_score else 0.5 end as u_score from " +
      " (select userid,friendid,a_score,t_score from v2)a " +
      "left outer join ( select creatorid, u_score from mutual)b on a.friendid = b.creatorid ").
      withColumn("score", computeFinalScore2($"a_score", $"t_score",$"u_score"))

  }
  def computeFinalScore2: UserDefinedFunction = udf((a: Double, t: Double, u: Double) => {

    val score = a * t * u

    f"${score}%1.8f".toDouble
  })

}
