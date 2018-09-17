package com.netease.music.algo.event

import com.netease.music.algo.event.path.OutputPath._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number

object Rank {

  val conf = new SparkConf()
  val spark = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()
  case class FriendIds(rank: Int, friendid: String)


  def main(args: Array[String]): Unit = {

    import spark.implicits._
    val windowSpec = Window.partitionBy("userId").orderBy($"score".desc)

    spark.read.parquet(outputPathAT1).select($"userId", row_number().over(windowSpec).as("rn"), $"friendid").rdd.
      map(row => (row.getString(0), FriendIds(row.getInt(1), row.getString(2)))).
      groupByKey.
      map({ case (key, value) =>
        (key, value.toArray
          .sortWith(_.rank < _.rank)
          .map(_.friendid).mkString(","))
      }).toDF("userid","friendids")
    .write.mode(SaveMode.Overwrite).parquet(affinityRank)

  }


}
