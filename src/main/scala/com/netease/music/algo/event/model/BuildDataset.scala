package com.netease.music.algo.event.model

import java.io.InputStream

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

import scala.collection.mutable
import Path._
import UserFunctions._

/**
  * Created by tao.li on 2018/05/28.
  * res4: Long = 2851569
  * scala> res1.where("label < 1").count
res5: Long = 1419177
  */
object BuildDataset {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._


    val eventFeatureInput="/user/ndir/music_recommend/event/event_feature_warehouse"

    val userFeatureInput="/user/ndir/music_recommend/event/user_feature_warehouse"
    val commonSchema = StructType(Array(StructField("userid", StringType, nullable = false), StructField("result", StringType, nullable = false)))


//    val samples = spark.read.option("sep", "\t").schema(sampleSchema).csv(sampleInput)
    val samples = spark.read.parquet(sampleInput).where("label = 0").sample(withReplacement = false, 0.03).
      union( spark.read.parquet(sampleInput).where("label = 1"))

    val stream : InputStream = BuildDataset.getClass.getResourceAsStream("/category_list")
    val lines = scala.io.Source.fromInputStream(stream).getLines

    val categoryMap:mutable.HashMap[String, Int] = mutable.HashMap()
    for (line <- lines.zipWithIndex) {
      categoryMap.put(line._1, line._2)
    }
    // 处理动态特征
    val eventFeature = spark.read.parquet(eventFeatureInput).na.fill(0, Seq("score", "zanCount", "commentCount", "forwardCount", "impressCount",
      "clickCount", "zanCountInRec", "commentCountInRec", "forwardCountInRec")).
      withColumn("smoothClickRate", computeSmoothRate(100, 1000)($"impressCount", $"clickCount")).
      withColumn("zanRate", computeSmoothRate(0, 1)($"impressCount", $"zanCount")).
      withColumn("commentRate", computeSmoothRate(0, 1)($"impressCount", $"commentCount")).
      withColumn("forwardRate", computeSmoothRate(0, 1)($"impressCount", $"forwardCount")).
      withColumn("encodedEventLanguage", transformLanguageEvent($"eventLanguage")).
      withColumn("encodedEventCategory", transformCategoryEvent(categoryMap)($"eventCategory")).
      select("eventId", "eventArtistId",
        "score", "zanCount", "commentCount", "forwardCount",
        "zanRate", "commentRate", "forwardRate",
        "smoothClickRate", "encodedEventLanguage", "encodedEventCategory",
        "ageEventClickRate_0", "ageEventClickRate_1", "ageEventClickRate_2", "ageEventClickRate_3",
        "genderEventClickRate_0", "genderEventClickRate_1", "genderEventClickRate_2")

    // 处理用户特征
    val userFeature = spark.read.parquet(userFeatureInput)
      .withColumn("encodedUserLanguage", transformLanguageUser($"userLanguage"))
      .withColumn("encodedUserLanguageInEvent", transformLanguageUserInEvent($"userLanguageInEvent"))
      .withColumn("encodedUserCategory", transformCategoryUser(categoryMap)($"userCategory"))
      .select("userId", "subArtists", "recArtists",
        "encodedUserLanguage", "encodedUserLanguageInEvent", "encodedUserCategory",
        "ageType", "gender"
      )

    // 联合之后计算交叉特征
    val finalDataset = samples.join(eventFeature, Seq("eventId"), "left_outer")
      .join(userFeature, Seq("userId"), "left_outer")
      .withColumn("isSubArtist", containsArtist($"eventArtistId", $"subArtists"))
      .withColumn("isRecArtist", containsArtist($"eventArtistId", $"recArtists"))
      .withColumn("crossLanguageInMusicVector", multiplyVectors(5)($"encodedEventLanguage", $"encodedUserLanguage"))
      .withColumn("crossLanguageInEventVector", multiplyVectors(5)($"encodedEventLanguage", $"encodedUserLanguageInEvent"))
      .withColumn("crossCategoryVector", multiplyVectors(categoryMap.size)($"encodedEventCategory", $"encodedUserCategory"))
      .withColumn("clickRateForUserAge", computeClickRateForUserAge($"ageType", $"ageEventClickRate_0", $"ageEventClickRate_1", $"ageEventClickRate_2", $"ageEventClickRate_3"))
      .withColumn("clickRateForUserGender", computeClickRateForUserGender($"gender", $"genderEventClickRate_0", $"genderEventClickRate_1", $"genderEventClickRate_2"))
      .na.fill(0, Seq("score", "zanCount", "commentCount", "forwardCount", "impressCount", "zanRate", "commentRate", "forwardRate"))
      .na.fill(0, Seq("isSubArtist", "isRecArtist"))
      .na.fill(0.1, Seq("smoothClickRate"))
//      .na.fill(0.1, Seq("clickRateForUserAge", "clickRateForUserGender"))
      .select($"eventId", $"userId", $"label".cast(DoubleType).as("label"), $"logDay",
        $"score".as("originScore"),
        log1p($"score").as("logScore"),
        log10Int($"score").as("score"),
        log10Int($"zanCount").as("zanCount"),
        log10Int($"commentCount").as("commentCount"),
        log10Int($"forwardCount").as("forwardCount"),
        $"smoothClickRate", $"zanRate", $"commentRate", $"forwardRate",
        fillNAVectors2(4)($"clickRateForUserAge").as("clickRateForUserAge"),
        fillNAVectors2(3)($"clickRateForUserGender").as("clickRateForUserGender"),
        fillNAVectors(5)($"encodedEventLanguage").as("eventLanguageVector"),
        fillNAVectors(categoryMap.size)($"encodedEventCategory").as("eventCategoryVector"),
        $"isSubArtist", $"isRecArtist",
        $"crossLanguageInMusicVector", $"crossLanguageInEventVector", $"crossCategoryVector").
      withColumn("score",computeEncodedFeature1($"score")).
      withColumn("zanCount",computeEncodedFeature1($"zanCount")).
      withColumn("commentCount",computeEncodedFeature1($"commentCount")).
      withColumn("forwardCount",computeEncodedFeature1($"forwardCount")).
      withColumn("clickRateForUserAge",computeEncodedFeature1($"clickRateForUserAge")).
      withColumn("clickRateForUserGender",computeEncodedFeature1($"clickRateForUserGender")).
      withColumn("eventLanguageVector",computeEncodedFeature1($"eventLanguageVector")).
      withColumn("eventCategoryVector",computeEncodedFeature1($"eventCategoryVector")).
      withColumn("crossLanguageInMusicVector",computeEncodedFeature1($"crossLanguageInMusicVector")).
      withColumn("crossLanguageInEventVector",computeEncodedFeature1($"crossLanguageInEventVector")).
      withColumn("crossCategoryVector",computeEncodedFeature1($"crossCategoryVector")).
      drop("eventId").drop("userid").drop("originScore")


      finalDataset.write.mode(SaveMode.Overwrite).parquet(dataSetOutput2)

  }

}
