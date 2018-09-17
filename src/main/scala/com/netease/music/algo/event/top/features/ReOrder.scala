package com.netease.music.algo.event.top.features

import com.netease.music.algo.event.path.OutputPath._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object ReOrder {
  def main(args: Array[String]): Unit = {
    import spark.implicits._

    val clk = spark.read.parquet(clkRatePath.concat(args(0))).
      withColumn("clkRate", $"click" / $"impress").drop("impress", "click").
      groupBy("userId").agg(collect_set($"friendid").alias("friendids"))
    clk.createOrReplaceTempView("clk")
    val rank = spark.read.parquet(affinityRank)
    rank.createOrReplaceTempView("rank")
    spark.udf.register("reorder", reorder)

    val result = spark.sql("select a.userid, case when b.friendids is not null then reorder(a.friendids, b.friendids) else a.friendids end as result " +
      "from (select userid, friendids from rank)a left outer join(select userid, friendids from clk)b on a.userid = b.userid")
    result.write.mode(SaveMode.Overwrite).parquet(affinityRank1)
  }

  val reorder = ((result: String, friendids: Seq[String]) => {
    var res = result
    val index1 = result.indexOf(",")
    val index2 = result.indexOf(",", index1 + 1)
    if (friendids != null && index1 != -1 && index2 != -1) {
      res = result.drop(index2 + 1)
      var leftStr = ""
      var rightStr = ""
      var friendid = result.substring(0, index1)
      if (!friendids.contains(friendid)) {
        leftStr = leftStr.concat(friendid).concat(",")
      }
      else {
        rightStr = rightStr.concat(",").concat(friendid)
      }
      friendid = result.substring(index1 + 1, index2)
      if (!friendids.contains(friendid)) {
        leftStr = leftStr.concat(friendid).concat(",")
      }
      else {
        rightStr = rightStr.concat(",").concat(friendid)
      }
      res = leftStr.concat(res).concat(rightStr)
    }
    res
  })
  val conf = new SparkConf()
  val spark = SparkSession
    .builder()
    .config(conf).enableHiveSupport()
    .getOrCreate()

}
