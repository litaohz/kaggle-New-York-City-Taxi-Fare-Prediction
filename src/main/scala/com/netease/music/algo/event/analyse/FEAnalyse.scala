package com.netease.music.algo.event.analyse

import com.netease.music.algo.event.schema.BaseSchema._
import com.netease.music.algo.event.udf.Udf1.{extractVideoId, splitVideos}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object FEAnalyse {
  def main(args: Array[String]): Unit = {
    println("hello")
    val verifiedEventInput = "/db_dump/music_ndir/Music_BackendRcmdEventInfo_whitelist_all_with_effectiveTime"
    val conf = new SparkConf()
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    import spark.implicits._

    val meta = spark.read.json("/db_dump/music_ndir/Music_EventMeta/2018-03-27").withColumnRenamed("id","eventid").
      where("Type in(17,18,19,21,28,36,13)")
    val verifiedBackendRcmdEventInfoTable = spark.read.option("sep", "\t").
      schema(verifiedBackendRcmdEventInfoTableSchema).csv(verifiedEventInput)
    val res = meta. join(verifiedBackendRcmdEventInfoTable,"eventid")
    res.where("Type in(17)").show(10)//3507153762 3507158662
    res.where("Type in(18)").show(10)//3507172343  3507179245
    res.where("Type in(19)").show(10)//3507178335 3507158760
    res.where("Type in(36)").show(10)//3507191079 3507155880

    meta.where("Type in(21)").show(10)//3507141992 3507181295
    meta.where("Type in(28)").show(10)//3507156690 3507181308
    meta.where("Type in(13)").show(10)//3507188169 3507189155
    meta.where("Type in(24)").show(10)//3507188169 3507189155


  }
}
