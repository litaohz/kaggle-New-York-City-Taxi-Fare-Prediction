package com.netease.music.algo.event

import java.util.{Arrays, Collections}

import com.netease.music.algo.event.schema.BaseSchema.tranEventSchema
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{collect_list, collect_set, concat_ws, udf}
import com.netease.music.algo.event.udf.ComputeScore._
import com.netease.music.algo.event.compute.SnsPatch._
import com.netease.music.algo.event.path.OutputPath._
import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.types.LongType

import scala.collection.JavaConverters._
object Patch1  {

  val conf = new SparkConf()
  val spark = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()
  import spark.implicits._

  val output = "music_recommend/event/sns/output1/"

  def main(args: Array[String]): Unit = {

    spark.read.parquet(affinityRank1).
      withColumn("result", cutAndAppend($"result"))
      .select(concat_ws("\t", $"userId", $"result"))
    .write.mode(SaveMode.Overwrite).option("compression","gzip").text(output.concat(args(0)))

  }


}
