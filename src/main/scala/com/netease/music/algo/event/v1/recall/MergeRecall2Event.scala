package com.netease.music.algo.event.v1.recall

import com.netease.music.algo.event.compute.SnsBase.getNdaysShift
import com.netease.music.algo.event.path.DeletePath.delete
import com.netease.music.algo.event.udf.Udf1._
import com.netease.music.algo.event.v1.Path._
import org.apache.spark.sql.functions.{collect_list, concat_ws, explode}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object MergeRecall2Event {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate


    import spark.implicits._


    spark.read.parquet(matchResult.concat("/type=follow")).
      select("userid", "match").withColumn("match", explode(splitResultAll(($"match")))).
      withColumn("creatorid", extractResult(0)($"match")).
      withColumn("relatedid", extractResult1(1)($"match")).
      join(spark.read.parquet(eventPoolPath), "creatorid").select("userid", "creatorid", "relatedid", "eventid").
      withColumn("eventid",$"eventid".cast(StringType)).
      groupBy("userId").agg(collect_list($"eventid").alias("eventid")).
      select($"userId", concat_ws(",", $"eventid").alias("result")).
      select(concat_ws("\t", $"userid", $"result"))
      .write.option("compression", "gzip").mode(SaveMode.Overwrite).text(matchResultForEventTmp)


  }


}
