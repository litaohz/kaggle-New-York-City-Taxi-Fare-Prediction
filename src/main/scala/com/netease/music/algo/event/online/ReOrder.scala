package com.netease.music.algo.event.online

import com.netease.music.algo.event.compute.SnsPatch.getPatchData
import com.netease.music.algo.event.path.OutputPath._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object ReOrder {
  def main(args: Array[String]): Unit = {
    import spark.implicits._
    val patchStr = getPatchData(args)

    val clk = spark.read.parquet(batchClk).
      withColumn("clk_rate", $"click" / $"impress").drop("impress", "click").
      select($"userId", concat_ws(":", $"friendid", $"clk_rate").alias("friendinfo")).
      groupBy("userId").agg(collect_set($"friendinfo").alias("friendinfo"))

    clk.createOrReplaceTempView("clk")
    val rank = spark.read.parquet(affinityRankRollingUpdate)
    rank.createOrReplaceTempView("rank")
    spark.udf.register("reorder", reorder)

    val result = spark.sql("select a.userid,  reorder(result, friendinfo)  as result " +
      "from (select userid, result from rank)a  join(select userid, friendinfo from clk)b on a.userid = b.userid")
    result.write.mode(SaveMode.Overwrite).parquet(affinityRankRollingUpdateTmp)


  }

  val reorder = ((result: String, friendinfos: Seq[String]) => {
    val scores = new scala.collection.mutable.HashMap[String, Double]
    val friendinfoTuples = friendinfos.foreach(x => {
      val score = x.split(":")
      scores.put(score(0), score(1).toDouble)
    })
    val resArray = result.split(",").map(x => {
      (x, if (scores.contains(x)) scores(x) else 0.1)
    })
    val resList = resArray.toSeq.filter(_._2 > 0.05).sortWith(_._2 > _._2).map(_._1)
    resList.mkString(",")
  })
  val conf = new SparkConf()
  val spark = SparkSession
    .builder()
    .config(conf).enableHiveSupport()
    .getOrCreate()

}
