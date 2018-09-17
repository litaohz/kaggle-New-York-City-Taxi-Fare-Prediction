package com.netease.music.algo.event.kaggle

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object Feature {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._


    spark.read.parquet("data/test/kaggle/basicinfo").
      withColumn("trip_distance",computeDist($"pickup_longitude",$"pickup_latitude",$"dropoff_longitude",$"dropoff_latitude")).
      filter("trip_distance > 0\n" +
        "  AND fare_amount >= 2.5\n  " +
        "AND pickup_longitude > -78\n " +
        " AND pickup_longitude < -70\n  " +
        "AND dropoff_longitude > -78\n  " +
        "AND dropoff_longitude < -70\n  " +
        "AND pickup_latitude > 37\n  " +
        "AND pickup_latitude < 45\n  " +
        "AND dropoff_latitude > 37\n  " +
        "AND dropoff_latitude < 45\n " +
        " AND passenger_count > 0").repartition(5).write.parquet("data/test/kaggle/basicinfo1")
  }

  /*
    dist = np.degrees(
    np.arccos(
    np.sin(np.radians(lat1)) * np.sin(np.radians(lat2)) +
     np.cos(np.radians(lat1)) * np.cos(np.radians(lat2)) * np.cos(np.radians(lon2 - lon1))))
     * 60 * 1.515 * 1.609344
     * http://www.cocoachina.com/ios/20141118/10238.html

   */
  def computeDist = udf((lon1: Double, lat1: Double, lon2: Double, lat2: Double) => {
    val cosab = Math.cos((lon2 - lon1).toRadians) * Math.cos(lat2.toRadians) * Math.cos(lat1.toRadians) +
      Math.sin(lat1.toRadians) * Math.sin(lat2.toRadians)
    Math.acos(cosab).toDegrees * 60 * 1.515 * 1.609344
  })


}
