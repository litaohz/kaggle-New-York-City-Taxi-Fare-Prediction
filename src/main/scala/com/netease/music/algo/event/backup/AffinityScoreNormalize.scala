package com.netease.music.algo.event.backup

import com.netease.music.algo.event.path.OutputPath._
import com.netease.music.algo.event.udf.ComputeScore._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
/*
scala> res1.where("n_score < -1").count
res23: Long = 6104890

scala> res1.where("n_score < -1.05").count
res24: Long = 206711

scala> res1.where("n_score < -1.1").count
res25: Long = 0
 */

object AffinityScoreNormalize {

  val conf = new SparkConf()
  val spark = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()

  val hc = new org.apache.spark.sql.hive.HiveContext(spark.sparkContext)

  def main(args: Array[String]): Unit = {
    import spark.implicits._
    val atu = spark.read.parquet(outputPathATUBak)
    atu.createOrReplaceTempView("atu")
    val normalize = spark.sql("select mean(score) as mean, stddev(score) as stddev from atu").collect()
    val mean = normalize(0).getAs[Double](0)
    val stddev = normalize(0).getAs[Double](1)
    atu.withColumn("n_score",normalizeScore(mean, stddev)($"score")).
      write.mode(SaveMode.Overwrite).parquet(outputPathATN)
  }
}
