package com.netease.music.algo.event.kaggle

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object TimeFeature {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._

  val format = "yyyy-MM-dd HH:mm:ss 'UTC'"
    spark.read.parquet("data/test/kaggle/basicinfo1").
      withColumn("time_feature",computeTimeFeature(format)($"pickup_datetime")).
      select($"*",
      col("time_feature").getItem(0).as("hour_passed"),
      col("time_feature").getItem(1).as("week_month"),
      col("time_feature").getItem(2).as("week_year"),
      col("time_feature").getItem(3).as("day_week"),
      col("time_feature").getItem(4).as("am_pm")
    ).drop("time_feature").repartition(5).write.parquet("data/test/kaggle/basicinfo2")

  }

  /*

   */
  def computeTimeFeature(format:String) = udf((timeStr: String) => {

      var date = null
      val dateFormat: SimpleDateFormat = new SimpleDateFormat(format)
      val calendar = Calendar.getInstance
      calendar.setTime(dateFormat.parse(timeStr))
      /*
      nytime
       */
      calendar.set(Calendar.HOUR, calendar.get(Calendar.HOUR) - 5)
    Seq(calendar.get(Calendar.HOUR),calendar.get(Calendar.WEEK_OF_MONTH),
        calendar.get(Calendar.WEEK_OF_YEAR),calendar.get(Calendar.DAY_OF_WEEK),calendar.get(Calendar.AM_PM))
  })


}
