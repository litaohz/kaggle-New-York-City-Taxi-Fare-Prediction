package com.netease.music.algo.event.sns

import com.netease.music.algo.event.AffinityScore.spark
import com.netease.music.algo.event.schema.BaseSchema.tranEventSchema
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object BaseInteraction {

  val conf = new SparkConf()
  val spark = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()

  import spark.implicits._

  val outputPath = "music_recommend/event/sns/interaction"

  def main(args: Array[String]): Unit = {
    import spark.implicits._
    val evt = spark.read.option("sep", "\t").schema(tranEventSchema).csv("music_recommend/event/event_meta/current_event_meta").
      withColumn("resType",split($"resourceTypeNid"))

  }

  def split: UserDefinedFunction = udf((info: String) => {
    info.split(":")(0)
  })
}
