package com.netease.music.algo.event

import com.netease.music.algo.event.schema.BaseSchema.tranEventSchema
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{collect_list, concat_ws}
import com.netease.music.algo.event.udf.ComputeScore._
import com.netease.music.algo.event.compute.SnsPatch._
import com.netease.music.algo.event.path.OutputPath._

object Patch {

  val conf = new SparkConf()
  val spark = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()

  import spark.implicits._

  val output = "music_recommend/event/sns/output/"

  def main(args: Array[String]): Unit = {

    val patchStr = getPatchData(args)
   spark.read.parquet(affinityRank1).
      withColumn("result", doCut(patchStr, args(0), "o", "p")($"result")).

    write.mode(SaveMode.Overwrite).option("compression", "gzip").text(output.concat(args(0)))

  }
}
