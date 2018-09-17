package com.netease.music.algo.event.kaggle

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object CollectData {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._
    case class TaxiInfo(fare_amount: Double,
                        pickup_datetime: String,
                        pickup_longitude: Double,
                        pickup_latitude: Double,
                        dropoff_longitude: Double,
                        dropoff_latitude: Double,
                        passenger_count: Int)

    case class TaxiInfoPred(
                        pickup_datetime: String,
                        pickup_longitude: Double,
                        pickup_latitude: Double,
                        dropoff_longitude: Double,
                        dropoff_latitude: Double,
                        passenger_count: Int)
    spark.sparkContext.textFile("data/test/kaggle/train.csv").filter(x => {
      !x.startsWith("key")
    }).map(x => {
      x.split(",").filter(!_.isEmpty)
    }).filter(_.size == 8).map(info => {
      TaxiInfo(info(1).toDouble, info(2), info(3).toDouble, info(4).toDouble, info(5).toDouble, info(6).toDouble, info(7).toInt)
    }).toDF.repartition(5).write.parquet("data/test/kaggle/basicinfo")

    spark.sparkContext.textFile("data/test/kaggle/test.csv").filter(x => {
      !x.startsWith("key")
    }).map(x => {
      x.split(",")
    }).map(info => {
      TaxiInfoPred(info(1), info(2).toDouble, info(3).toDouble, info(4).toDouble, info(5).toDouble, info(6).toInt)
    }).toDF
  }
}
