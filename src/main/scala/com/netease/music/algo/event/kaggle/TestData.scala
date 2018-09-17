package com.netease.music.algo.event.kaggle

import com.netease.music.algo.event.kaggle.Feature.computeDist
import com.netease.music.algo.event.kaggle.TimeFeature.computeTimeFeature
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, concat_ws}

object TestData {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._

    val format = "yyyy-MM-dd HH:mm:ss 'UTC'"

    case class TaxiInfoPred(
                        pickup_datetime: String,
                        pickup_longitude: Double,
                        pickup_latitude: Double,
                        dropoff_longitude: Double,
                        dropoff_latitude: Double,
                        passenger_count: Int)

    val info1 = spark.sparkContext.textFile("data/test/kaggle/test.csv").filter(x => {
      !x.startsWith("key")
    }).map(x => {
      x.split(",")
    }).map(info => {
      TaxiInfoPred(info(1), info(2).toDouble, info(3).toDouble, info(4).toDouble, info(5).toDouble, info(6).toInt)
    }).toDF

    val testDataset = info1.
      withColumn("trip_distance",computeDist($"pickup_longitude",$"pickup_latitude",$"dropoff_longitude",$"dropoff_latitude")).
      withColumn("time_feature",computeTimeFeature(format)($"pickup_datetime")).
      select($"*",
        col("time_feature").getItem(0).as("hour_passed"),
        col("time_feature").getItem(1).as("week_month"),
        col("time_feature").getItem(2).as("week_year"),
        col("time_feature").getItem(3).as("day_week"),
        col("time_feature").getItem(4).as("am_pm")
      ).drop("time_feature")

    testDataset.select(concat_ws("\t",
      $"pickup_datetime",
      $"passenger_count",
      $"trip_distance",
      $"hour_passed",
      $"week_month",
      $"week_year",
      $"day_week",
      $"am_pm"
    )).repartition(1).
      write.text("data/test/kaggle/basicinfo3/pred")

  }
}
