package com.netease.music.algo.event.kaggle

import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{MinMaxScaler, MinMaxScalerModel, VectorAssembler}
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.sql.functions.concat_ws
import org.apache.spark.sql.{SaveMode, SparkSession}

object Train1 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    import spark.implicits._

    val fullDataset = spark.read.parquet("data/test/kaggle/basicinfo2")

    val trainingDataset = fullDataset.sample(withReplacement = false, 0.8)
    val testDataset = fullDataset.sample(withReplacement = false, 0.2)
    trainingDataset.select(concat_ws("\t",
      $"passenger_count",
      $"trip_distance",
      $"hour_passed",
      $"week_month",
      $"week_year",
      $"day_week",
      $"am_pm",$"fare_amount"
    )).repartition(1).
      write.mode(SaveMode.Overwrite).text("data/test/kaggle/basicinfo3/train")

    testDataset.select(concat_ws("\t",
      $"passenger_count",
      $"trip_distance",
      $"hour_passed",
      $"week_month",
      $"week_year",
      $"day_week",
      $"am_pm",$"fare_amount"
    )).repartition(1).
      write.mode(SaveMode.Overwrite).text("data/test/kaggle/basicinfo3/test")

  }

  /*

   */


}
