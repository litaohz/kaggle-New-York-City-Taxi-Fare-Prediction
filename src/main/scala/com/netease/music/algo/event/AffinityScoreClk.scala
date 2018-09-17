package com.netease.music.algo.event

import com.netease.music.algo.event.path.OutputPath._
import com.netease.music.algo.event.udf.RankAgg
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object AffinityScoreClk {

  val conf = new SparkConf()
  val spark = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()

  val hc = new org.apache.spark.sql.hive.HiveContext(spark.sparkContext)

  def main(args: Array[String]): Unit = {
    import spark.implicits._
    val atn= spark.read.parquet(outputPathATN)
    val clk = spark.read.parquet(clkRatePath).withColumnRenamed("userid", "friendid").withColumn("clkrate", $"click"/$"impress").
      drop("impress").drop("click")
//    atn.join(clk, "friendid").withColumn("clk")

  }
  def rank(): org.apache.spark.sql.DataFrame = {

    spark.udf.register("rankagg", new RankAgg)

    val v3 = spark.sql("select userid, rankagg(friendid, score) as result from v3 group by userid")
    v3
  }
}
