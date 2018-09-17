package com.netease.music.algo.event.popular

import java.io.InputStream

import com.netease.music.algo.event.model.BuildDataset
import com.netease.music.algo.event.model.Path.sampleInput
import com.netease.music.algo.event.model.UserFunctions._
import com.netease.music.algo.event.path.OutputPath._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{concat_ws, max}

import scala.collection.mutable

object EventTrainingData {

  def main(args: Array[String]): Unit = {
    import spark.implicits._
    val label = spark.read.parquet(sampleInput.concat("ds=2018-08-14"))

    val eventFeatureInput="/user/ndir/music_recommend/event/event_feature_warehouse"
    val stream : InputStream = BuildDataset.getClass.getResourceAsStream("/category_list")
    val lines = scala.io.Source.fromInputStream(stream).getLines

    val categoryMap:mutable.HashMap[String, Int] = mutable.HashMap()
    for (line <- lines.zipWithIndex) {
      categoryMap.put(line._1, line._2)
    }


    val eventFeature = spark.read.parquet(eventFeatureInput).na.fill(0, Seq("score", "zanCount", "commentCount", "forwardCount", "impressCount",
      "clickCount", "zanCountInRec", "commentCountInRec", "forwardCountInRec")).
      withColumn("smoothClickRate", computeSmoothRate(100, 1000)($"impressCount", $"clickCount")).
      withColumn("zanRate", computeSmoothRate(0, 1)($"impressCount", $"zanCount")).
      withColumn("commentRate", computeSmoothRate(0, 1)($"impressCount", $"commentCount")).
      withColumn("forwardRate", computeSmoothRate(0, 1)($"impressCount", $"forwardCount")).
      withColumn("encodedEventLanguage", transformLanguageEvent($"eventLanguage")).
      withColumn("encodedEventCategory", transformCategoryEvent(categoryMap)($"eventCategory"))


    val sample = eventFeature.
      withColumn("encoded_fresh_0",computeEncodedFeature($"refreshEventClickRate_0")).
      withColumn("encoded_fresh_1",computeEncodedFeature($"refreshEventClickRate_1")).
      withColumn("encoded_fresh_2",computeEncodedFeature($"refreshEventClickRate_2")).
      withColumn("encoded_fresh_3",computeEncodedFeature($"refreshEventClickRate_3")).
      withColumn("encoded_age_0",computeEncodedFeature($"ageEventClickRate_0")).
      withColumn("encoded_age_1",computeEncodedFeature($"ageEventClickRate_1")).
      withColumn("encoded_age_2",computeEncodedFeature($"ageEventClickRate_2")).
      withColumn("encoded_age_3",computeEncodedFeature($"ageEventClickRate_3")).
      withColumn("encoded_gender_0",computeEncodedFeature($"genderEventClickRate_0")).
      withColumn("encoded_gender_1",computeEncodedFeature($"genderEventClickRate_1")).
      withColumn("encoded_gender_2",computeEncodedFeature($"genderEventClickRate_2")).
      withColumn("encoded_language",computeEncodedFeature1($"encodedEventLanguage")).
      withColumn("encoded_cate",computeEncodedFeature1($"encodedEventCategory")).join(label,"eventid").
    select(concat_ws("\t",$"eventId", $"eventArtistId",
      $"score", $"zanCount", $"commentCount", $"forwardCount",
      $"zanRate", $"commentRate", $"forwardRate",
      $"smoothClickRate",$"encoded_fresh_0",$"encoded_fresh_1",$"encoded_fresh_2",$"encoded_fresh_3",
      $"encoded_age_0",$"encoded_age_1",$"encoded_age_2",$"encoded_age_3",
      $"encoded_gender_0",$"encoded_gender_1",$"encoded_gender_2",$"encoded_language",$"encoded_cate",$"label"))
    sample.sample(withReplacement = false, 0.8).repartition(1).
    write.mode(SaveMode.Overwrite).text(popularEventFeature.concat("train"))
    sample.sample(withReplacement = false, 0.2).repartition(1).
      write.mode(SaveMode.Overwrite).text(popularEventFeature.concat("dev"))
    sample.sample(withReplacement = false, 0.2).repartition(1).
      write.mode(SaveMode.Overwrite).text(popularEventFeature.concat("test"))

  }
  val conf = new SparkConf()
  val spark = SparkSession
    .builder()
    .config(conf).enableHiveSupport()
    .getOrCreate()

}
