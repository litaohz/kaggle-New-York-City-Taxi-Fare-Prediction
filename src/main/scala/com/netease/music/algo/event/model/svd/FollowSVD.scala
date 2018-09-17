package com.netease.music.algo.event.model.svd

import com.netease.music.algo.event.schema.BaseSchema._
import com.netease.music.algo.event.v1.Path._
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object FollowSVD {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val userItemRatingData = spark.read.option("sep", "\t").schema(commonSchema7_1).csv(followCfPre)
    val userIdMapping = spark.read.parquet("/user/ndir/music_recommend/feed_video/follow/als_direct_follow_triple.all/userIdMapping")
    val itemMapping = spark.read.parquet("/user/ndir/music_recommend/feed_video/follow/als_direct_follow_triple.all/friendMapping")


    val test = userItemRatingData.join(userIdMapping, "useridstr").join(itemMapping, "friendidstr").select("userId", "friendId")
    val model = ALSModel.load(modelOutputAls)
    val predictions = model.transform(test)
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error = $rmse")


  }
}
